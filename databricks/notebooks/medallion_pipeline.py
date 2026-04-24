# Databricks notebook source
# DevRadar — Medallion Pipeline (Bronze -> Silver -> Gold)
#
# Compatível com Databricks Community Edition.
# Usa boto3 + dbutils.secrets para acessar o S3.
# Salva tabelas Delta no metastore local (default database).
#
# Parâmetros (via widget ou Lambda notebook_params):
#   arquivo_novo  — key S3 do arquivo de posts (ex: reddit/python/date=2026-03-29/raw_....json)
#
# Módulos: %run medallion_schemas | medallion_helpers | medallion_transforms
#
# Tabelas criadas/atualizadas:
#   default.devradar_bronze_posts
#   default.devradar_bronze_comments
#   default.devradar_silver_posts
#   default.devradar_silver_comments
#   default.devradar_gold_subreddit_week
#   default.devradar_gold_top_commenters

# COMMAND ----------
# MAGIC %run ./medallion_schemas

# COMMAND ----------
# MAGIC %run ./medallion_helpers

# COMMAND ----------
# MAGIC %run ./medallion_transforms

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# 1. Parâmetros

dbutils.widgets.text("arquivo_novo", "")
arquivo_novo = dbutils.widgets.get("arquivo_novo")

if not arquivo_novo:
    dbutils.notebook.exit("Nenhum arquivo informado. Execução abortada.")

print(f"Arquivo recebido: {arquivo_novo}")

parsed = parse_arquivo_path(arquivo_novo)
if not parsed:
    dbutils.notebook.exit(f"Path não reconhecido: {arquivo_novo}")

subreddit, ingest_date, filename = parsed

print(f"  Subreddit  : r/{subreddit}")
print(f"  Date       : {ingest_date}")
print(f"  Filename   : {filename}")

# COMMAND ----------
# 2. Conexão S3

s3 = get_s3_client(dbutils)

# COMMAND ----------
# 3. Bronze — Posts

print(f"\n{'=' * 60}")
print(f"[1/6] BRONZE POSTS — Baixando {arquivo_novo}")
print(f"{'=' * 60}")

posts_data = download_json(s3, arquivo_novo)
posts = posts_data.get("posts", [])
print(f"  {len(posts)} posts baixados")

if not posts:
    dbutils.notebook.exit(f"Arquivo sem posts: {arquivo_novo}")

df_bronze_posts = spark.createDataFrame(posts, schema=BRONZE_POSTS_SCHEMA)
df_bronze_posts = df_bronze_posts.withColumn("_ingest_date", F.lit(ingest_date))
df_bronze_posts = df_bronze_posts.withColumn("_source_file", F.lit(arquivo_novo))
df_bronze_posts = df_bronze_posts.withColumn("_loaded_at", F.current_timestamp())

df_bronze_posts.createOrReplaceTempView("_new_bronze_posts")
spark.sql("""
    MERGE INTO default.devradar_bronze_posts AS target
    USING _new_bronze_posts AS source
    ON target.id = source.id AND target._ingest_date = source._ingest_date
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"  Merge em default.devradar_bronze_posts ({len(posts)} registros)")

# COMMAND ----------
# 4. Bronze — Comentários

print(f"\n{'=' * 60}")
print("[2/6] BRONZE COMMENTS — Buscando comentários do mesmo diretório")
print(f"{'=' * 60}")

directory = arquivo_novo.rsplit("/", 1)[0]
comment_files = list_files(s3, f"{directory}/", r"^comments_.*\.json$")

all_comments = []
for cf in comment_files:
    print(f"  Baixando: {cf}")
    cdata = download_json(s3, cf)
    comments = cdata.get("comments", [])
    for c in comments:
        c["subreddit"] = subreddit
    all_comments.extend(comments)

print(f"  Total: {len(all_comments)} comentários de {len(comment_files)} arquivo(s)")

has_comments = len(all_comments) > 0

if has_comments:
    df_bronze_comments = spark.createDataFrame(all_comments, schema=BRONZE_COMMENTS_SCHEMA)
    df_bronze_comments = df_bronze_comments.withColumn("_ingest_date", F.lit(ingest_date))
    df_bronze_comments = df_bronze_comments.withColumn("_source_file", F.lit(directory))
    df_bronze_comments = df_bronze_comments.withColumn("_loaded_at", F.current_timestamp())

    w_dedup = Window.partitionBy("id", "_ingest_date").orderBy(
        F.col("_loaded_at").desc_nulls_last(),
        F.col("post_id").desc_nulls_last(),
    )
    df_bronze_comments = (
        df_bronze_comments.withColumn("_dedup_rn", F.row_number().over(w_dedup))
        .filter(F.col("_dedup_rn") == 1)
        .drop("_dedup_rn")
    )

    df_bronze_comments.createOrReplaceTempView("_new_bronze_comments")
    spark.sql("""
        MERGE INTO default.devradar_bronze_comments AS target
        USING _new_bronze_comments AS source
        ON target.id = source.id AND target._ingest_date = source._ingest_date
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("  Merge em default.devradar_bronze_comments")
else:
    print("  Nenhum comentário encontrado — continuando só com posts.")
    df_bronze_comments = None

# COMMAND ----------
# 5. Silver — Posts

print(f"\n{'=' * 60}")
print("[3/6] SILVER POSTS — Limpeza e deduplicação")
print(f"{'=' * 60}")

df_silver_posts = build_silver_posts(df_bronze_posts, ingest_date)

silver_post_count = df_silver_posts.count()
print(f"  {silver_post_count} posts após limpeza")

df_silver_posts.createOrReplaceTempView("_new_silver_posts")
spark.sql("""
    MERGE INTO default.devradar_silver_posts AS target
    USING _new_silver_posts AS source
    ON target.id = source.id
    WHEN MATCHED AND source.ingest_date >= target.ingest_date
      THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("  Merge em default.devradar_silver_posts")

# COMMAND ----------
# 6. Silver — Comentários

print(f"\n{'=' * 60}")
print("[4/6] SILVER COMMENTS — Limpeza e deduplicação")
print(f"{'=' * 60}")

df_silver_comments = None

if has_comments:
    df_silver_comments = build_silver_comments(df_bronze_comments, ingest_date)

    silver_comment_count = df_silver_comments.count()
    print(f"  {silver_comment_count} comentários após limpeza")

    df_silver_comments.createOrReplaceTempView("_new_silver_comments")
    spark.sql("""
        MERGE INTO default.devradar_silver_comments AS target
        USING _new_silver_comments AS source
        ON target.id = source.id
        WHEN MATCHED AND source.ingest_date >= target.ingest_date
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("  Merge em default.devradar_silver_comments")
else:
    print("  Sem comentários — pulando.")

# COMMAND ----------
# 7. Gold — Métricas por subreddit/semana

print(f"\n{'=' * 60}")
print("[5/6] GOLD — Métricas semanais por subreddit")
print(f"{'=' * 60}")

df_gold_week = build_gold_subreddit_week(df_silver_posts, df_silver_comments, ingest_date)

df_gold_week.createOrReplaceTempView("_new_gold_week")
spark.sql("""
    MERGE INTO default.devradar_gold_subreddit_week AS target
    USING _new_gold_week AS source
    ON target.week_start = source.week_start AND target.subreddit = source.subreddit
    WHEN MATCHED AND source.computed_at > COALESCE(target.computed_at, TIMESTAMP '1900-01-01')
      THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("  Merge em default.devradar_gold_subreddit_week")
df_gold_week.display()

# COMMAND ----------
# 8. Gold — Top comentaristas

print(f"\n{'=' * 60}")
print("[6/6] GOLD — Top comentaristas")
print(f"{'=' * 60}")

df_gold_commenters = build_gold_top_commenters(df_silver_comments, ingest_date)

if df_gold_commenters is not None:
    df_gold_commenters.createOrReplaceTempView("_new_gold_commenters")
    spark.sql("""
        MERGE INTO default.devradar_gold_top_commenters AS target
        USING _new_gold_commenters AS source
        ON target.subreddit = source.subreddit AND target.author = source.author
        WHEN MATCHED AND source.computed_at > COALESCE(target.computed_at, TIMESTAMP '1900-01-01')
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("  Merge em default.devradar_gold_top_commenters")
    df_gold_commenters.limit(20).display()
else:
    print("  Sem comentários — pulando.")

# COMMAND ----------
# Resumo

print(f"\n{'=' * 60}")
print("PIPELINE CONCLUÍDO")
print(f"{'=' * 60}")
print(f"  Subreddit     : r/{subreddit}")
print(f"  Data          : {ingest_date}")
print(f"  Posts Bronze   : {len(posts)}")
print(f"  Posts Silver   : {silver_post_count}")
print(f"  Comentários   : {len(all_comments)}")
print("  Tabelas atualizadas:")
print("    - default.devradar_bronze_posts")
print("    - default.devradar_bronze_comments")
print("    - default.devradar_silver_posts")
print("    - default.devradar_silver_comments")
print("    - default.devradar_gold_subreddit_week")
print("    - default.devradar_gold_top_commenters")
