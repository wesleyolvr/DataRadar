"""
DevRadar — DAG de ingestão diária do Reddit → S3 (camada Bronze / Raw).

Fluxo por subreddit:
  1. extract_<sub>  — chama extract_reddit.extract_subreddit()
  2. upload_<sub>   — serializa JSON em memória e faz PUT no S3

Caminho no S3:
  s3://{BUCKET}/reddit/{subreddit}/date=YYYY-MM-DD/raw.json
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import task

from airflow import DAG

sys.path.insert(0, "/opt/airflow/scripts")

logger = logging.getLogger(__name__)

SUBREDDITS = ["dataengineering", "python"]

S3_BUCKET = os.getenv("DEVRADAR_S3_BUCKET", "devradar-raw")

DEFAULT_ARGS = {
    "owner": "devradar",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def build_subreddit_tasks(sub: str) -> None:
    """Cria o par extract → upload para um subreddit, evitando closure no loop."""

    @task(task_id=f"extract_{sub}")
    def extract(**context) -> list[dict]:
        from extract_reddit import extract_subreddit

        execution_date = context["ds"]
        logger.info("Extraindo r/%s para execution_date=%s", sub, execution_date)

        posts = extract_subreddit(
            subreddit=sub,
            sort="hot",
            max_pages=3,
            per_page=100,
        )

        logger.info("r/%s: %d posts extraídos", sub, len(posts))
        return posts

    @task(task_id=f"upload_{sub}")
    def upload_to_s3(posts: list[dict], **context) -> str:
        import boto3

        execution_date = context["ds"]
        s3_key = f"reddit/{sub}/date={execution_date}/raw.json"

        payload = {
            "subreddit": sub,
            "execution_date": execution_date,
            "extracted_at": datetime.now(tz=None).isoformat(),
            "count": len(posts),
            "posts": posts,
        }
        body = json.dumps(payload, ensure_ascii=False, default=str)

        logger.info(
            "Upload para s3://%s/%s (%d posts, %.1f KB)",
            S3_BUCKET, s3_key, len(posts), len(body) / 1024,
        )

        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

        destination = f"s3://{S3_BUCKET}/{s3_key}"
        logger.info("Upload concluído: %s", destination)
        return destination

    posts_data = extract()
    upload_to_s3(posts=posts_data)


with DAG(
    dag_id="devradar_reddit_ingestion",
    default_args=DEFAULT_ARGS,
    description="Extrai posts do Reddit e envia JSON cru para o S3 (Bronze layer)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["devradar", "reddit", "ingestion", "bronze"],
    max_active_runs=1,
) as dag:

    for _subreddit in SUBREDDITS:
        build_subreddit_tasks(_subreddit)
