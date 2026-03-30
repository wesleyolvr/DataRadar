"""Dados mockados para Silver e Gold a partir dos posts Bronze reais."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

TECH_TOOLS = {
    "spark": "Apache Spark",
    "pyspark": "Apache Spark",
    "airflow": "Apache Airflow",
    "dagster": "Dagster",
    "prefect": "Prefect",
    "dbt": "dbt",
    "kafka": "Apache Kafka",
    "flink": "Apache Flink",
    "databricks": "Databricks",
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
    "redshift": "Redshift",
    "postgres": "PostgreSQL",
    "postgresql": "PostgreSQL",
    "python": "Python",
    "sql": "SQL",
    "docker": "Docker",
    "kubernetes": "Kubernetes",
    "k8s": "Kubernetes",
    "terraform": "Terraform",
    "aws": "AWS",
    "gcp": "GCP",
    "azure": "Azure",
    "delta lake": "Delta Lake",
    "iceberg": "Apache Iceberg",
    "polars": "Polars",
    "pandas": "Pandas",
    "duckdb": "DuckDB",
    "streamlit": "Streamlit",
    "fastapi": "FastAPI",
    "fivetran": "Fivetran",
    "airbyte": "Airbyte",
    "looker": "Looker",
    "power bi": "Power BI",
    "tableau": "Tableau",
    "superset": "Apache Superset",
    "mlflow": "MLflow",
    "redis": "Redis",
    "mongodb": "MongoDB",
    "clickhouse": "ClickHouse",
    "trino": "Trino",
}

_TOOL_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in TECH_TOOLS) + r")\b",
    re.IGNORECASE,
)


def _extract_tools(text: str) -> list[str]:
    """Extrai nomes de ferramentas do texto via regex."""
    found = _TOOL_PATTERN.findall(text.lower())
    canonical = {TECH_TOOLS[m] for m in found}
    return sorted(canonical)


def transform_to_silver(bronze_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Simula a camada Silver: limpeza + extração de ferramentas."""
    silver: list[dict[str, Any]] = []
    for post in bronze_posts:
        if post.get("author") in ("[deleted]", "[removed]", None):
            continue
        if not post.get("title"):
            continue

        text = f"{post.get('title', '')} {post.get('selftext', '')}"
        tools = _extract_tools(text)

        silver.append({
            "id": post["id"],
            "subreddit": post.get("subreddit"),
            "title": post.get("title"),
            "selftext_clean": (post.get("selftext", "") or "")[:500],
            "author": post.get("author"),
            "score": post.get("score", 0),
            "num_comments": post.get("num_comments", 0),
            "created_date": post.get("created_date"),
            "flair": post.get("flair"),
            "tools_mentioned": tools,
            "tools_count": len(tools),
        })
    return silver


def aggregate_to_gold(silver_posts: list[dict[str, Any]]) -> dict[str, Any]:
    """Simula a camada Gold: métricas agregadas de negócio."""
    if not silver_posts:
        return {"tool_rankings": [], "subreddit_rankings": [], "summary": {}}

    tool_counter: Counter[str] = Counter()
    tool_score: dict[str, int] = {}
    sub_counter: Counter[str] = Counter()
    sub_score: dict[str, int] = {}

    for post in silver_posts:
        sub = post.get("subreddit", "unknown")
        score = post.get("score", 0)
        sub_counter[sub] += 1
        sub_score[sub] = sub_score.get(sub, 0) + score

        for tool in post.get("tools_mentioned", []):
            tool_counter[tool] += 1
            tool_score[tool] = tool_score.get(tool, 0) + score

    tool_rankings = [
        {
            "tool": tool,
            "mentions": count,
            "total_score": tool_score.get(tool, 0),
            "avg_score": round(tool_score.get(tool, 0) / count, 1) if count else 0,
        }
        for tool, count in tool_counter.most_common(15)
    ]

    subreddit_rankings = [
        {
            "subreddit": sub,
            "posts": count,
            "total_score": sub_score.get(sub, 0),
            "avg_score": round(sub_score.get(sub, 0) / count, 1) if count else 0,
        }
        for sub, count in sub_counter.most_common(10)
    ]

    scores = [p.get("score", 0) for p in silver_posts]

    return {
        "tool_rankings": tool_rankings,
        "subreddit_rankings": subreddit_rankings,
        "summary": {
            "total_posts": len(silver_posts),
            "unique_tools": len(tool_counter),
            "unique_subreddits": len(sub_counter),
            "avg_score": round(sum(scores) / len(scores), 1),
            "posts_with_tools": sum(1 for p in silver_posts if p.get("tools_count", 0) > 0),
        },
    }
