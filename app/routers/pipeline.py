"""Endpoint do pipeline — estado das 3 camadas Medallion."""

from __future__ import annotations

from fastapi import APIRouter
from services.bronze_reader import get_all_posts_flat, list_subreddits
from services.mock_layers import aggregate_to_gold, transform_to_silver

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"])


@router.get("/status")
def pipeline_status():
    subs = list_subreddits()
    bronze_total = sum(s["total_posts"] for s in subs)

    bronze_posts = get_all_posts_flat(limit=500)
    silver_posts = transform_to_silver(bronze_posts)
    gold = aggregate_to_gold(silver_posts)

    sample_bronze = bronze_posts[0] if bronze_posts else None
    sample_silver = silver_posts[0] if silver_posts else None

    return {
        "layers": [
            {
                "name": "Bronze",
                "status": "active",
                "description": "Dados brutos extraídos da API pública do Reddit",
                "tech": "Airflow + Python requests → JSON local",
                "records": bronze_total,
                "subreddits": len(subs),
                "sample": sample_bronze,
            },
            {
                "name": "Silver",
                "status": "mock",
                "description": "Dados limpos com extração de ferramentas via NLP",
                "tech": "Databricks + PySpark → Delta Tables (planejado)",
                "records": len(silver_posts),
                "tools_detected": gold["summary"].get("unique_tools", 0),
                "sample": sample_silver,
            },
            {
                "name": "Gold",
                "status": "mock",
                "description": "Métricas de negócio: rankings, tendências, agregações",
                "tech": "dbt → Tabelas de negócio (planejado)",
                "records": gold["summary"].get("total_posts", 0),
                "sample": {
                    "top_tool": gold["tool_rankings"][0] if gold["tool_rankings"] else None,
                    "top_subreddit": gold["subreddit_rankings"][0] if gold["subreddit_rankings"] else None,
                    "summary": gold["summary"],
                },
            },
        ],
        "silver_posts": silver_posts[:30],
        "gold": gold,
    }
