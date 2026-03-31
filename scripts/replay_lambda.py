"""Replay: invoca o Lambda manualmente para o raw_*.json mais recente de cada subreddit.

Simula o S3 Event Notification que o Lambda normalmente recebe, permitindo
reprocessar arquivos que foram uploadados antes do Lambda estar funcionando.

Uso:
    python scripts/replay_lambda.py                  # dry-run (só mostra o que faria)
    python scripts/replay_lambda.py --execute        # invoca o Lambda de verdade
    python scripts/replay_lambda.py --execute --delay 5   # 5s entre invocações
"""

import argparse
import json
import os
import sys
import time

import boto3

BUCKET = os.getenv("DEVRADAR_S3_BUCKET", "devradar-raw")
LAMBDA_FUNCTION = os.getenv("DEVRADAR_LAMBDA_NAME", "devradar-s3-trigger")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


def _list_subreddits(s3_client) -> list[str]:
    """Lista os subreddits (prefixos de primeiro nível) no bucket."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix="reddit/", Delimiter="/")

    subs = []
    for page in pages:
        for prefix in page.get("CommonPrefixes", []):
            sub = prefix["Prefix"].split("/")[1]
            if sub:
                subs.append(sub)
    return sorted(subs)


def _get_latest_raw(s3_client, subreddit: str) -> str | None:
    """Retorna a key do raw_*.json mais recente de um subreddit."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=f"reddit/{subreddit}/")

    latest = None
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if (
                filename.startswith("raw_")
                and filename.endswith(".json")
                and (latest is None or key > latest)
            ):
                latest = key
    return latest


def get_latest_raw_per_subreddit(s3_client) -> dict[str, str]:
    """Retorna o raw_*.json mais recente por subreddit."""
    subs = _list_subreddits(s3_client)
    print(f"  Encontrados {len(subs)} subreddits. Buscando arquivo mais recente de cada...")

    latest: dict[str, str] = {}
    for i, sub in enumerate(subs, 1):
        key = _get_latest_raw(s3_client, sub)
        if key:
            latest[sub] = key
        if i % 10 == 0:
            print(f"  ... {i}/{len(subs)} subreddits verificados")

    return dict(sorted(latest.items()))


def build_s3_event(key: str) -> dict:
    """Constrói um payload de S3 Event Notification idêntico ao real."""
    return {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": BUCKET},
                    "object": {"key": key},
                },
            }
        ]
    }


def main():
    parser = argparse.ArgumentParser(description="Replay Lambda para raw_*.json mais recentes")
    parser.add_argument("--execute", action="store_true", help="Invocar o Lambda de verdade (sem isso, só dry-run)")
    parser.add_argument("--delay", type=int, default=3, help="Segundos entre invocações (default: 3)")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=REGION)
    latest = get_latest_raw_per_subreddit(s3)

    print(f"\n{'='*60}")
    print(f"  Replay Lambda — {len(latest)} subreddits encontrados")
    print(f"  Bucket: {BUCKET}")
    print(f"  Lambda: {LAMBDA_FUNCTION}")
    print(f"  Modo: {'EXECUTE' if args.execute else 'DRY-RUN'}")
    print(f"{'='*60}\n")

    if not args.execute:
        for sub, key in latest.items():
            print(f"  [dry-run] {sub:30s} -> {key}")
        print(f"\nTotal: {len(latest)} arquivos. Use --execute para invocar o Lambda.")
        return

    lambda_client = boto3.client("lambda", region_name=REGION)
    success = 0
    errors = 0

    for i, (sub, key) in enumerate(latest.items(), 1):
        event = build_s3_event(key)
        print(f"  [{i}/{len(latest)}] {sub:30s} -> {key}")

        try:
            response = lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION,
                InvocationType="Event",
                Payload=json.dumps(event).encode(),
            )
            status = response.get("StatusCode", 0)
            if status == 202:
                print("           OK Invocação aceita (async)")
                success += 1
            else:
                print(f"           WARN Status inesperado: {status}")
                errors += 1
        except Exception as e:
            print(f"           FAIL Erro: {e}")
            errors += 1

        if i < len(latest):
            time.sleep(args.delay)

    print(f"\n{'='*60}")
    print(f"  Resultado: {success} sucesso, {errors} erros")
    print(f"{'='*60}")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
