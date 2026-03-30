"""
DataRadar — DAG agendada de ingestão do Reddit (a cada hora).

Lê a lista de subreddits da Airflow Variable `devradar_subreddits`
(JSON array) e executa o pipeline completo: extração → validação →
salvamento local → comentários → upload S3.

Airflow Variable esperada:
  devradar_subreddits = ["dataengineering", "python", "rust"]

Se a Variable não existir, usa o default: ["dataengineering", "python"].

Parâmetros conservadores para respeitar rate limits da API pública:
  - max_pages = 2  (menos requests por subreddit)
  - top_k_comments = 20  (menos requests de comentários)
  - upload_s3 = true  (sempre sobe para o S3)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models import Variable

from airflow import DAG

sys.path.insert(0, "/opt/airflow/scripts")

logger = logging.getLogger(__name__)

LOCAL_DATA_DIR = Path("/opt/airflow/data")
S3_BUCKET = os.getenv("DEVRADAR_S3_BUCKET", "devradar-raw")

REQUIRED_FIELDS = {"id", "subreddit", "title", "created_utc"}

DEFAULT_SUBS = ["dataengineering", "python"]
SORT = "hot"
MAX_PAGES = 2
TOP_K_COMMENTS = 20
MIN_COMMENTS = 5
COMMENT_DEPTH = 3

DEFAULT_ARGS = {
    "owner": "devradar",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


@task
def get_subreddits() -> list[str]:
    """Lê subreddits da Airflow Variable (JSON array)."""
    raw = Variable.get("devradar_subreddits", default_var=json.dumps(DEFAULT_SUBS))
    try:
        subs = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        logger.warning("Variable 'devradar_subreddits' inválida (%s). Usando default.", raw)
        subs = DEFAULT_SUBS

    if not isinstance(subs, list) or not subs:
        subs = DEFAULT_SUBS

    logger.info("Subreddits agendados: %s", subs)
    return subs


@task(pool="reddit_api", max_active_tis_per_dag=2)
def extract(sub: str, **context) -> dict:
    """Extrai posts de um subreddit."""
    from extract_reddit import extract_subreddit

    execution_date = context["ds"]
    logger.info(
        "Extraindo r/%s (sort=%s, max_pages=%d) — %s",
        sub, SORT, MAX_PAGES, execution_date,
    )

    posts = extract_subreddit(
        subreddit=sub,
        sort=SORT,
        max_pages=MAX_PAGES,
        per_page=100,
    )

    logger.info("r/%s: %d posts extraídos", sub, len(posts))
    return {"subreddit": sub, "posts": posts}


@task
def validate(result: dict) -> dict:
    """Valida schema. Subreddits sem posts são pulados (não travam o pipeline)."""
    sub = result["subreddit"]
    posts = result["posts"]

    if not posts:
        logger.warning("r/%s: 0 posts — subreddit será pulado nesta execução.", sub)
        return {"subreddit": sub, "posts": [], "_skip": True}

    invalid_count = sum(
        1 for p in posts if REQUIRED_FIELDS - set(p.keys())
    )

    scores = [p.get("score", 0) for p in posts]
    logger.info(
        "Validação r/%s: %d posts, %d inválidos, score médio %.1f",
        sub, len(posts), invalid_count, sum(scores) / len(scores),
    )

    if invalid_count > len(posts) * 0.5:
        logger.warning(
            "r/%s: %d/%d posts inválidos — subreddit será pulado.",
            sub, invalid_count, len(posts),
        )
        return {"subreddit": sub, "posts": [], "_skip": True}

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

    skip_result = {
        "subreddit": sub, "posts": [], "out_dir": "",
        "posts_file": None, "s3_key_posts": None, "_skip": True,
    }

    if result.get("_skip") or not posts:
        logger.info("r/%s: pulando save_local (sem posts válidos).", sub)
        return skip_result

    cache_path = LOCAL_DATA_DIR / "reddit" / sub / "posts_cache.json"
    current_fp = _build_posts_fingerprint(posts)
    cached_fp = _load_posts_cache(cache_path)

    new_ids = current_fp - cached_fp
    if not new_ids:
        logger.info(
            "r/%s: nenhum post novo (%d posts, mesmos IDs) — pulando snapshot.",
            sub, len(posts),
        )
        return skip_result

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
        "r/%s snapshot salvo: %s (%d posts, %.1f KB)",
        sub, out_path, len(posts), len(body) / 1024,
    )
    return {
        "subreddit": sub,
        "posts": posts,
        "out_dir": str(out_dir),
        "posts_file": str(out_path),
        "s3_key_posts": f"reddit/{sub}/date={execution_date}/{filename}",
    }


@task(pool="reddit_api", max_active_tis_per_dag=1)
def extract_and_save_comments(save_result: dict, **context) -> dict:
    """Extrai comentários com cache e salva snapshot."""
    from extract_reddit import extract_comments_for_posts

    sub = save_result["subreddit"]
    base_result = {
        "subreddit": sub,
        "posts_file": save_result.get("posts_file"),
        "s3_key_posts": save_result.get("s3_key_posts"),
        "comments_file": None,
        "s3_key_comments": None,
    }

    if save_result.get("_skip"):
        logger.info("r/%s: pulando comentários (sem posts válidos).", sub)
        return base_result

    posts = save_result["posts"]
    out_dir = Path(save_result["out_dir"])
    execution_date = context["ds"]
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%dT%H_%M_%S")

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
        min_comments=MIN_COMMENTS,
        top_k=TOP_K_COMMENTS,
        comment_depth=COMMENT_DEPTH,
    )

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(updated_cache, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    if not comments:
        logger.info("r/%s: nenhum comentário novo.", sub)
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

    logger.info(
        "r/%s comentários salvos: %s (%d, %.1f KB)",
        sub, out_path, len(comments), len(body) / 1024,
    )
    base_result["comments_file"] = str(out_path)
    base_result["s3_key_comments"] = f"reddit/{sub}/date={execution_date}/{filename}"
    return base_result


@task
def upload_to_s3(file_info: dict) -> str:
    """Upload dos arquivos para S3."""
    import boto3

    sub = file_info["subreddit"]

    if not file_info.get("posts_file") and not file_info.get("comments_file"):
        logger.info("r/%s: nada para enviar ao S3.", sub)
        return "skipped"

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
            logger.warning("Arquivo não encontrado: %s", local_path)
            continue

        body = path.read_bytes()
        logger.info("Upload s3://%s/%s (%.1f KB)", S3_BUCKET, s3_key, len(body) / 1024)

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=body,
            ContentType="application/json",
        )
        uploaded.append(f"s3://{S3_BUCKET}/{s3_key}")

    if uploaded:
        logger.info("r/%s: %d arquivo(s) enviados.", sub, len(uploaded))
        return f"uploaded: {', '.join(uploaded)}"

    return "nothing_to_upload"


with DAG(
    dag_id="devradar_reddit_scheduled",
    default_args=DEFAULT_ARGS,
    description="[PROD] Extrai Reddit a cada hora — subreddits via Variable",
    schedule="0 */1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["devradar", "reddit", "scheduled", "prod"],
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    subs_list = get_subreddits()
    raw_results = extract.expand(sub=subs_list)
    validated_results = validate.expand(result=raw_results)
    saved_results = save_local.expand(result=validated_results)
    comment_results = extract_and_save_comments.expand(save_result=saved_results)
    upload_to_s3.expand(file_info=comment_results)
