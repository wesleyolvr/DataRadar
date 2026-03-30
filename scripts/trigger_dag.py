"""
Trigger rápido da DAG local do DataRadar via API REST do Airflow.

Uso:
  python trigger_dag.py rust golang devops
  python trigger_dag.py machinelearning --sort top --pages 5
  python trigger_dag.py dataengineering --no-comments
  python trigger_dag.py python --min-comments 10 --top-k 30 --depth 5
  python trigger_dag.py dataengineering --upload-s3
  python trigger_dag.py                  (usa defaults: dataengineering, python)
"""

import argparse
import json
import sys

import requests

AIRFLOW_URL = "http://localhost:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"
DAG_ID = "devradar_reddit_ingestion_local"


def trigger(
    subreddits: list[str],
    sort: str,
    max_pages: int,
    extract_comments: bool,
    min_comments: int,
    top_k_comments: int,
    comment_depth: int,
    upload_s3: bool,
) -> None:
    endpoint = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"

    conf = {
        "subreddits": subreddits,
        "sort": sort,
        "max_pages": max_pages,
        "extract_comments": extract_comments,
        "min_comments": min_comments,
        "top_k_comments": top_k_comments,
        "comment_depth": comment_depth,
        "upload_s3": upload_s3,
    }

    print(f"Triggering DAG '{DAG_ID}'")
    print(f"  subreddits        : {subreddits}")
    print(f"  sort              : {sort}")
    print(f"  max_pages         : {max_pages}")
    print(f"  extract_comments  : {extract_comments}")
    if extract_comments:
        print(f"  min_comments      : {min_comments}")
        print(f"  top_k_comments    : {top_k_comments}")
        print(f"  comment_depth     : {comment_depth}")
    print(f"  upload_s3         : {upload_s3}")

    resp = requests.post(
        endpoint,
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        headers={"Content-Type": "application/json"},
        json={"conf": conf},
        timeout=15,
    )

    if resp.status_code in (200, 201):
        data = resp.json()
        run_id = data.get("dag_run_id", "???")
        state = data.get("state", "???")
        print("\nDAG disparada com sucesso!")
        print(f"  run_id : {run_id}")
        print(f"  state  : {state}")
        print(f"\nAcompanhe em: {AIRFLOW_URL}/dags/{DAG_ID}/grid")
    else:
        print(f"\nErro {resp.status_code}:")
        print(json.dumps(resp.json(), indent=2, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trigger da DAG DataRadar")
    parser.add_argument(
        "subreddits",
        nargs="*",
        default=["dataengineering", "python"],
        help="Subreddits para extrair (ex: rust golang devops)",
    )
    parser.add_argument(
        "--sort",
        choices=["hot", "new", "top", "rising"],
        default="hot",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        choices=range(1, 11),
        metavar="1-10",
    )
    parser.add_argument(
        "--no-comments",
        action="store_true",
        help="Pular extração de comentários",
    )
    parser.add_argument(
        "--min-comments",
        type=int,
        default=5,
        help="Mín. de comentários para buscar (default: 5)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=50,
        help="Máx. de posts para buscar comentários (default: 50)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=3,
        choices=range(1, 11),
        metavar="1-10",
        help="Profundidade de replies (default: 3)",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Enviar arquivos para S3 após salvar localmente",
    )

    args = parser.parse_args()
    trigger(
        args.subreddits,
        args.sort,
        args.pages,
        extract_comments=not args.no_comments,
        min_comments=args.min_comments,
        top_k_comments=args.top_k,
        comment_depth=args.depth,
        upload_s3=args.upload_s3,
    )
