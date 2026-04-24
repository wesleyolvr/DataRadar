# Databricks notebook source
# DevRadar — S3 (boto3 + secrets) e parsing do path S3

import json
import re

import boto3

BUCKET = "devradar-raw"
_PATH_RE = re.compile(r"reddit/([^/]+)/date=(\d{4}-\d{2}-\d{2})/(.+)")


def get_s3_client(dbutils):
    access_key = dbutils.secrets.get(scope="aws_credentials", key="s3_access_key")
    secret_key = dbutils.secrets.get(scope="aws_credentials", key="s3_secret_key")
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def download_json(s3, key):
    """Baixa um JSON do S3 e retorna como dict."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def list_files(s3, prefix, pattern):
    """Lista arquivos no S3 cujo nome de arquivo corresponde ao padrão regex."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [c["Key"] for c in resp.get("Contents", [])]
    return [k for k in keys if re.search(pattern, k.split("/")[-1])]


def parse_arquivo_path(arquivo_novo):
    """
    Extrai subreddit, date e filename de keys no formato:
    reddit/{sub}/date={date}/{filename}
    Retorna (subreddit, ingest_date, filename) ou None.
    """
    m = _PATH_RE.match(arquivo_novo)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)
