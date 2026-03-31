"""Endpoint do pipeline — estado das 3 camadas Medallion."""

from __future__ import annotations

import json
import logging
import os

from fastapi import APIRouter
from services.bronze_reader import get_all_posts_flat, list_subreddits
from services.databricks_client import (
    fetch_gold_subreddit_week,
    fetch_gold_summary,
    fetch_gold_top_commenters,
    fetch_silver_posts,
    is_configured,
)
from services.mock_layers import aggregate_to_gold, transform_to_silver

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"])


def _get_silver_gold_from_databricks() -> tuple[list, dict] | None:
    if not is_configured():
        return None
    try:
        silver_posts = fetch_silver_posts(limit=30)
        if not silver_posts:
            return None

        subreddit_week = fetch_gold_subreddit_week()
        top_commenters = fetch_gold_top_commenters()
        summary = fetch_gold_summary()

        gold = {
            "subreddit_week": subreddit_week,
            "top_commenters": top_commenters,
            "summary": summary,
        }
        return silver_posts, gold
    except Exception as e:
        logger.warning("Databricks indisponivel, usando fallback mock: %s", e)
        return None


@router.get("/status")
def pipeline_status():
    subs = list_subreddits()
    bronze_total = sum(s["total_posts"] for s in subs)
    bronze_posts = get_all_posts_flat(limit=500)
    sample_bronze = bronze_posts[0] if bronze_posts else None

    db_result = _get_silver_gold_from_databricks()

    if db_result:
        silver_posts, gold = db_result
        silver_status = "active"
        gold_status = "active"
        silver_tech = "Databricks + PySpark + Delta Lake"
        gold_tech = "Databricks SQL Warehouse (Serverless)"
    else:
        silver_posts = transform_to_silver(bronze_posts)
        gold_mock = aggregate_to_gold(silver_posts)
        gold = {
            "subreddit_week": gold_mock.get("subreddit_rankings", []),
            "top_commenters": [],
            "summary": gold_mock.get("summary", {}),
        }
        silver_status = "mock"
        gold_status = "mock"
        silver_tech = "Mock local (regex sobre Bronze)"
        gold_tech = "Mock local (agregacao sobre Silver)"

    sample_silver = silver_posts[0] if silver_posts else None

    return {
        "layers": [
            {
                "name": "Bronze",
                "status": "active",
                "description": "Dados brutos extraidos da API publica do Reddit",
                "tech": "Airflow + Python requests -> JSON local + S3",
                "records": bronze_total,
                "subreddits": len(subs),
                "sample": sample_bronze,
            },
            {
                "name": "Silver",
                "status": silver_status,
                "description": "Posts e comentarios limpos, deduplicados",
                "tech": silver_tech,
                "records": len(silver_posts),
                "sample": sample_silver,
            },
            {
                "name": "Gold",
                "status": gold_status,
                "description": "Metricas por subreddit/semana, top commenters",
                "tech": gold_tech,
                "records": gold.get("summary", {}).get("total_posts", 0),
                "sample": {
                    "top_subreddit": gold["subreddit_week"][0] if gold.get("subreddit_week") else None,
                    "top_commenter": gold["top_commenters"][0] if gold.get("top_commenters") else None,
                    "summary": gold.get("summary", {}),
                },
            },
        ],
        "silver_posts": silver_posts[:30],
        "gold": gold,
        "insights": _load_insights(),
    }


def _load_insights() -> dict:
    path = os.path.join(os.path.dirname(__file__), "..", "static", "data.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("insights", {})
    except Exception:
        return {}
