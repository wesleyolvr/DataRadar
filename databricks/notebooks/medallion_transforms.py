# Databricks notebook source
# DevRadar — Transformações Silver / Gold (DataFrames puros, sem I/O)

from pyspark.sql import functions as F


def build_silver_posts(df_bronze_posts, ingest_date):
    return (
        df_bronze_posts.select(
            F.col("id"),
            F.col("subreddit"),
            F.col("title"),
            F.col("selftext"),
            F.col("author"),
            F.coalesce(F.col("score").cast("long"), F.lit(0)).alias("score"),
            F.coalesce(F.col("upvote_ratio").cast("double"), F.lit(0.0)).alias("upvote_ratio"),
            F.coalesce(F.col("num_comments").cast("long"), F.lit(0)).alias("num_comments"),
            F.col("created_utc").cast("double").alias("created_utc"),
            F.col("created_date"),
            F.col("permalink"),
            F.col("url"),
            F.col("flair"),
            F.coalesce(F.col("is_self"), F.lit(True)).alias("is_self"),
            F.lit(ingest_date).alias("ingest_date"),
        )
        .where(F.col("id").isNotNull())
        .where(F.col("title").isNotNull())
        .where(F.col("author") != "[deleted]")
        .dropDuplicates(["id"])
    )


def build_silver_comments(df_bronze_comments, ingest_date):
    return (
        df_bronze_comments.select(
            F.col("id"),
            F.col("post_id"),
            F.col("parent_id"),
            F.col("subreddit"),
            F.col("author"),
            F.col("body"),
            F.coalesce(F.col("score").cast("long"), F.lit(0)).alias("score"),
            F.coalesce(F.col("depth").cast("int"), F.lit(0)).alias("depth"),
            F.col("created_utc").cast("double").alias("created_utc"),
            F.col("created_date"),
            F.lit(ingest_date).alias("ingest_date"),
        )
        .where(F.col("id").isNotNull())
        .where(F.col("body").isNotNull())
        .where(F.col("author") != "[deleted]")
        .dropDuplicates(["id"])
    )


def build_gold_subreddit_week(df_silver_posts, df_silver_comments, ingest_date):
    df_gold_week = (
        df_silver_posts.withColumn(
            "week_start",
            F.date_trunc("week", F.to_timestamp(F.col("created_utc"))),
        )
        .groupBy("week_start", "subreddit")
        .agg(
            F.count(F.lit(1)).alias("post_count"),
            F.sum("score").alias("sum_score"),
            F.sum("num_comments").alias("sum_num_comments"),
            F.avg("score").alias("avg_score"),
            F.avg("num_comments").alias("avg_comments_per_post"),
        )
        .withColumn("week_start", F.date_format(F.col("week_start"), "yyyy-MM-dd"))
        .withColumn("ingest_date", F.lit(ingest_date))
        .withColumn("computed_at", F.current_timestamp())
    )

    if df_silver_comments is not None:
        comment_agg = (
            df_silver_comments.withColumn(
                "week_start",
                F.date_trunc("week", F.to_timestamp(F.col("created_utc"))),
            )
            .groupBy("week_start", "subreddit")
            .agg(
                F.count(F.lit(1)).alias("total_comments_extracted"),
                F.avg("score").alias("avg_comment_score"),
                F.countDistinct("author").alias("unique_commenters"),
            )
            .withColumn("week_start", F.date_format(F.col("week_start"), "yyyy-MM-dd"))
        )
        df_gold_week = df_gold_week.join(comment_agg, on=["week_start", "subreddit"], how="left")
    else:
        df_gold_week = (
            df_gold_week.withColumn("total_comments_extracted", F.lit(0).cast("long"))
            .withColumn("avg_comment_score", F.lit(0.0))
            .withColumn("unique_commenters", F.lit(0).cast("long"))
        )

    return df_gold_week


def build_gold_top_commenters(df_silver_comments, ingest_date):
    if df_silver_comments is None:
        return None
    return (
        df_silver_comments.groupBy("subreddit", "author")
        .agg(
            F.count(F.lit(1)).alias("comment_count"),
            F.sum("score").alias("total_score"),
            F.avg("score").alias("avg_score"),
            F.countDistinct("post_id").alias("posts_commented"),
        )
        .orderBy(F.col("comment_count").desc())
        .withColumn("ingest_date", F.lit(ingest_date))
        .withColumn("computed_at", F.current_timestamp())
    )
