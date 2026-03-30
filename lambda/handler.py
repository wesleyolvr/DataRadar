"""
DataRadar — AWS Lambda — Trigger Databricks Job quando novos arquivos chegam no S3.

Acionado por S3 Event Notification (ObjectCreated) no bucket devradar-raw.
Filtra apenas arquivos raw_*.json (posts) para evitar execuções duplicadas.
O notebook Databricks busca os comentários do mesmo diretório automaticamente.

Passa apenas o parâmetro `arquivo_novo` (key S3) para o notebook.
O notebook faz o parsing do path e baixa os dados via boto3.

Environment Variables (configurar no Lambda Console):
    DATABRICKS_DOMAIN  — ex: dbc-xxxxxxxx-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN   — Personal Access Token do Databricks
    JOB_ID             — ID numérico do Job no Databricks
"""

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

DATABRICKS_DOMAIN = os.environ.get("DATABRICKS_DOMAIN", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
JOB_ID = os.environ.get("JOB_ID", "")


def _should_process(key: str) -> bool:
    """Só processa arquivos raw_*.json (posts). Ignora comments e cache."""
    filename = key.rsplit("/", 1)[-1] if "/" in key else key
    return filename.startswith("raw_") and filename.endswith(".json")


def _is_valid_path(key: str) -> bool:
    """Verifica se o path segue o padrão esperado."""
    return bool(re.match(
        r"^reddit/[^/]+/date=\d{4}-\d{2}-\d{2}/.+\.json$", key
    ))


def _trigger_databricks(arquivo_novo: str) -> dict:
    """Chama POST /api/2.1/jobs/run-now no Databricks."""
    url = f"https://{DATABRICKS_DOMAIN}/api/2.1/jobs/run-now"

    payload = json.dumps({
        "job_id": int(JOB_ID),
        "notebook_params": {
            "arquivo_novo": arquivo_novo,
        },
    }).encode("utf-8")

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    req = urllib.request.Request(url, data=payload, headers=headers)

    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def lambda_handler(event, context):  # noqa: ARG001
    """Entry point do Lambda — acionado por S3 Event."""
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        print(f"Evento recebido: s3://{bucket}/{key}")

        if not _should_process(key):
            print(f"Ignorando (não é raw_*.json): {key}")
            continue

        if not _is_valid_path(key):
            print(f"Path não reconhecido: {key}")
            continue

        print(f"Triggering Databricks Job {JOB_ID} — arquivo: {key}")

        try:
            result = _trigger_databricks(key)
            db_run_id = result.get("run_id", "???")
            print(f"Job acionado com sucesso! Databricks Run ID: {db_run_id}")
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            print(f"Erro na API do Databricks: {e.code} — {error_body}")
            return {
                "statusCode": e.code,
                "body": error_body,
            }
        except Exception as e:
            print(f"Erro inesperado: {e}")
            raise

    return {
        "statusCode": 200,
        "body": json.dumps("Lambda executado com sucesso."),
    }
