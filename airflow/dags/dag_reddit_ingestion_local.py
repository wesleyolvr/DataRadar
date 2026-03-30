"""
DataRadar — DAG parametrizada de ingestão do Reddit.

Salva no disco local e, opcionalmente, faz upload para o S3 (Bronze).
Trigger manual com parâmetros configuráveis.

Parâmetros (passados via "Trigger DAG w/ config"):
  - subreddits        : lista de subreddits  (default: ["dataengineering", "python"])
  - sort              : hot | new | top | rising  (default: "hot")
  - max_pages         : 1–10  (default: 3)
  - extract_comments  : extrair comentários?  (default: true)
  - min_comments      : mín. comentários para buscar  (default: 5)
  - top_k_comments    : máx. posts para buscar comentários  (default: 50)
  - comment_depth     : profundidade de replies  (default: 3)
  - upload_s3         : enviar para S3?  (default: false)

Caminho local:
  /opt/airflow/data/reddit/{subreddit}/date=YYYY-MM-DD/
    raw_{timestamp}.json         ← posts
    comments_{timestamp}.json    ← comentários
    comments_cache.json          ← cache de estado

Caminho S3 (quando upload_s3=true):
  s3://{BUCKET}/reddit/{subreddit}/date=YYYY-MM-DD/
    raw_{timestamp}.json
    comments_{timestamp}.json
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

from airflow import DAG

sys.path.insert(0, "/opt/airflow/scripts")

logger = logging.getLogger(__name__)

LOCAL_DATA_DIR = Path("/opt/airflow/data")
S3_BUCKET = os.getenv("DEVRADAR_S3_BUCKET", "devradar-raw")

REQUIRED_FIELDS = {"id", "subreddit", "title", "created_utc"}

DEFAULT_ARGS = {
    "owner": "devradar",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@task
def get_subreddits(**context) -> list[str]:
    """Lê a lista de subreddits dos parâmetros do DAG Run."""
    subs = context["params"]["subreddits"]
    logger.info("Subreddits recebidos: %s", subs)
    return subs


@task
def extract(sub: str, **context) -> dict:
    """Extrai posts de um subreddit e retorna dict {sub, posts}."""
    from extract_reddit import extract_subreddit

    sort = context["params"]["sort"]
    max_pages = context["params"]["max_pages"]
    execution_date = context["ds"]

    logger.info(
        "Extraindo r/%s (sort=%s, max_pages=%d) para execution_date=%s",
        sub, sort, max_pages, execution_date,
    )

    posts = extract_subreddit(
        subreddit=sub,
        sort=sort,
        max_pages=max_pages,
        per_page=100,
    )

    logger.info("r/%s: %d posts extraídos", sub, len(posts))
    return {"subreddit": sub, "posts": posts}


@task
def validate(result: dict) -> dict:
    """Valida schema e loga estatísticas. Falha se 0 posts ou >50% inválidos."""
    sub = result["subreddit"]
    posts = result["posts"]

    if not posts:
        raise AirflowFailException(
            f"r/{sub}: extração retornou 0 posts — pipeline interrompido."
        )

    invalid_count = 0
    for post in posts:
        missing = REQUIRED_FIELDS - set(post.keys())
        if missing:
            invalid_count += 1
            logger.warning(
                "Post %s sem campos obrigatórios: %s",
                post.get("id", "???"), missing,
            )

    scores = [p.get("score", 0) for p in posts]
    avg_score = sum(scores) / len(scores)
    subs_found = {p.get("subreddit") for p in posts}

    logger.info("--- Validação r/%s ---", sub)
    logger.info("  Posts: %d", len(posts))
    logger.info("  Posts inválidos: %d", invalid_count)
    logger.info("  Score médio: %.1f", avg_score)
    logger.info("  Score máx: %d", max(scores))
    logger.info("  Subreddits: %s", subs_found)

    if invalid_count > len(posts) * 0.5:
        raise AirflowFailException(
            f"r/{sub}: {invalid_count}/{len(posts)} posts com schema inválido."
        )

    logger.info("r/%s: validação OK", sub)
    return result


def _build_posts_fingerprint(posts: list[dict]) -> set[str]:
    """Retorna conjunto de IDs dos posts (ignora score)."""
    return {p["id"] for p in posts if p.get("id")}


def _load_posts_cache(cache_path: Path) -> set[str]:
    """Lê o cache de IDs da última extração."""
    if not cache_path.exists():
        return set()
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set()
    except (json.JSONDecodeError, OSError):
        return set()


def _save_posts_cache(cache_path: Path, ids: set[str]) -> None:
    """Salva os IDs atuais no cache."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(sorted(ids)),
        encoding="utf-8",
    )


@task
def save_local(result: dict, **context) -> dict:
    """Salva snapshot append-only de posts, com cache para evitar duplicatas."""
    sub = result["subreddit"]
    posts = result["posts"]

    if not posts:
        logger.info("r/%s: sem posts para salvar.", sub)
        return {
            "subreddit": sub, "posts": [], "out_dir": "",
            "posts_file": None, "s3_key_posts": None, "_skip": True,
        }

    cache_path = LOCAL_DATA_DIR / "reddit" / sub / "posts_cache.json"
    current_fp = _build_posts_fingerprint(posts)
    cached_fp = _load_posts_cache(cache_path)

    new_ids = current_fp - cached_fp
    if not new_ids:
        logger.info(
            "r/%s: nenhum post novo (%d posts, mesmos IDs) — pulando snapshot.",
            sub, len(posts),
        )
        return {
            "subreddit": sub, "posts": [], "out_dir": "",
            "posts_file": None, "s3_key_posts": None, "_skip": True,
        }

    logger.info(
        "r/%s: %d posts novos (de %d total) — salvando snapshot.",
        sub, len(new_ids), len(posts),
    )

    execution_date = context["ds"]
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%dT%H_%M_%S")

    out_dir = LOCAL_DATA_DIR / "reddit" / sub / f"date={execution_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"raw_{timestamp}.json"
    out_path = out_dir / filename

    payload = {
        "subreddit": sub,
        "execution_date": execution_date,
        "snapshot_at": now.isoformat(),
        "count": len(posts),
        "posts": posts,
    }

    body = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    out_path.write_text(body, encoding="utf-8")

    _save_posts_cache(cache_path, current_fp)

    logger.info(
        "r/%s snapshot salvo em %s (%d posts, %.1f KB)",
        sub, out_path, len(posts), len(body) / 1024,
    )
    return {
        "subreddit": sub,
        "posts": posts,
        "out_dir": str(out_dir),
        "posts_file": str(out_path),
        "s3_key_posts": f"reddit/{sub}/date={execution_date}/{filename}",
    }


@task
def extract_and_save_comments(save_result: dict, **context) -> dict:
    """Extrai comentários dos posts (com cache) e salva snapshot."""
    from extract_reddit import extract_comments_for_posts

    sub = save_result["subreddit"]
    base_result = {
        "subreddit": sub,
        "posts_file": save_result.get("posts_file"),
        "s3_key_posts": save_result.get("s3_key_posts"),
        "comments_file": None,
        "s3_key_comments": None,
    }

    if not context["params"].get("extract_comments", True):
        logger.info("extract_comments=false — pulando extração de comentários.")
        return base_result

    posts = save_result["posts"]
    out_dir = Path(save_result["out_dir"])
    execution_date = context["ds"]
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%dT%H_%M_%S")

    min_comments = context["params"].get("min_comments", 5)
    top_k = context["params"].get("top_k_comments", 50)
    comment_depth = context["params"].get("comment_depth", 3)

    cache_path = LOCAL_DATA_DIR / "reddit" / sub / "comments_cache.json"
    cache: dict[str, int] = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Cache corrompido em %s — ignorando.", cache_path)

    comments, updated_cache, stats = extract_comments_for_posts(
        posts=posts,
        cache=cache,
        min_comments=min_comments,
        top_k=top_k,
        comment_depth=comment_depth,
    )

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(updated_cache, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logger.info("Cache atualizado em %s (%d entradas)", cache_path, len(updated_cache))

    if not comments:
        logger.info("r/%s: nenhum comentário novo extraído.", sub)
        return base_result

    filename = f"comments_{timestamp}.json"
    out_path = out_dir / filename
    payload = {
        "subreddit": sub,
        "execution_date": execution_date,
        "snapshot_at": now.isoformat(),
        "count": len(comments),
        "stats": stats,
        "comments": comments,
    }

    body = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    out_path.write_text(body, encoding="utf-8")

    size_kb = len(body) / 1024
    logger.info(
        "r/%s comentários salvos em %s (%d comentários, %.1f KB)",
        sub, out_path, len(comments), size_kb,
    )
    base_result["comments_file"] = str(out_path)
    base_result["s3_key_comments"] = f"reddit/{sub}/date={execution_date}/{filename}"
    return base_result


@task
def upload_to_s3(file_info: dict, **context) -> str:
    """Faz upload dos arquivos locais para o S3 (se upload_s3=true)."""
    if not context["params"].get("upload_s3", False):
        logger.info("upload_s3=false — pulando upload para S3.")
        return "skipped"

    import boto3

    sub = file_info["subreddit"]
    bucket = S3_BUCKET

    s3 = boto3.client("s3")
    uploaded = []

    for local_key, s3_key_key in [
        ("posts_file", "s3_key_posts"),
        ("comments_file", "s3_key_comments"),
    ]:
        local_path = file_info.get(local_key)
        s3_key = file_info.get(s3_key_key)

        if not local_path or not s3_key:
            continue

        path = Path(local_path)
        if not path.exists():
            logger.warning("Arquivo não encontrado: %s — pulando.", local_path)
            continue

        body = path.read_bytes()
        size_kb = len(body) / 1024

        logger.info(
            "Upload s3://%s/%s (%.1f KB)",
            bucket, s3_key, size_kb,
        )

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=body,
            ContentType="application/json",
        )

        destination = f"s3://{bucket}/{s3_key}"
        uploaded.append(destination)
        logger.info("Upload concluído: %s", destination)

    if uploaded:
        logger.info("r/%s: %d arquivo(s) enviados para S3.", sub, len(uploaded))
        return f"uploaded: {', '.join(uploaded)}"

    logger.info("r/%s: nenhum arquivo para enviar ao S3.", sub)
    return "nothing_to_upload"


with DAG(
    dag_id="devradar_reddit_ingestion_local",
    default_args=DEFAULT_ARGS,
    description="[DEV] Extrai posts do Reddit e salva JSON local — parametrizável",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["devradar", "reddit", "local", "dev"],
    max_active_runs=1,
    params={
        "subreddits": Param(
            default=["dataengineering", "python"],
            type="array",
            items={"type": "string"},
            description="Lista de subreddits para extrair (ex: ['rust', 'golang'])",
        ),
        "sort": Param(
            default="hot",
            enum=["hot", "new", "top", "rising"],
            description="Ordenação dos posts",
        ),
        "max_pages": Param(
            default=3,
            type="integer",
            minimum=1,
            maximum=10,
            description="Páginas por subreddit (cada página = ~100 posts)",
        ),
        "extract_comments": Param(
            default=True,
            type="boolean",
            description="Extrair comentários dos posts mais relevantes?",
        ),
        "min_comments": Param(
            default=5,
            type="integer",
            minimum=1,
            maximum=100,
            description="Mín. de comentários para buscar (filtra posts pouco discutidos)",
        ),
        "top_k_comments": Param(
            default=50,
            type="integer",
            minimum=1,
            maximum=300,
            description="Máx. de posts para buscar comentários (ordenados por engajamento)",
        ),
        "comment_depth": Param(
            default=3,
            type="integer",
            minimum=1,
            maximum=10,
            description="Profundidade máxima de replies nos comentários",
        ),
        "upload_s3": Param(
            default=False,
            type="boolean",
            description="Enviar arquivos para S3 após salvar localmente?",
        ),
    },
    render_template_as_native_obj=True,
) as dag:

    subs_list = get_subreddits()
    raw_results = extract.expand(sub=subs_list)
    validated_results = validate.expand(result=raw_results)
    saved_results = save_local.expand(result=validated_results)
    comment_results = extract_and_save_comments.expand(save_result=saved_results)
    upload_to_s3.expand(file_info=comment_results)
