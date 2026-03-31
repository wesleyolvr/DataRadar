"""Exporta snapshot dos dados Databricks para JSON estático (deploy Vercel)."""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from services.databricks_client import (  # noqa: E402
    fetch_gold_subreddit_week,
    fetch_gold_summary,
    fetch_gold_top_commenters,
    fetch_silver_posts,
)


def main() -> None:
    print("Conectando ao Databricks...")

    print("  Buscando Silver posts...")
    silver_posts = fetch_silver_posts(limit=50)
    print(f"  -> {len(silver_posts)} posts")

    for post in silver_posts:
        post["tools_mentioned"] = []
        post["tools_count"] = 0

    print("  Buscando Gold subreddit_week...")
    subreddit_week = fetch_gold_subreddit_week(limit=30)
    print(f"  -> {len(subreddit_week)} registros")

    subreddit_rankings = [
        {
            "subreddit": row["subreddit"],
            "posts": row["post_count"],
            "total_score": row["sum_score"],
            "avg_score": round(row["avg_score"], 1),
        }
        for row in subreddit_week
    ]

    print("  Buscando Gold top_commenters...")
    top_commenters = fetch_gold_top_commenters(limit=20)
    print(f"  -> {len(top_commenters)} registros")

    print("  Buscando Gold summary...")
    summary = fetch_gold_summary()
    print(f"  -> {summary}")

    gold = {
        "subreddit_rankings": subreddit_rankings,
        "subreddit_week": subreddit_week,
        "top_commenters": top_commenters,
        "tool_rankings": [],
        "summary": {
            "total_posts": summary.get("total_posts", 0),
            "unique_tools": 0,
            "unique_subreddits": summary.get("unique_subreddits", 0),
            "avg_score": summary.get("avg_score", 0),
            "posts_with_tools": 0,
            "total_comments": summary.get("total_comments", 0),
            "total_unique_commenters": summary.get("total_unique_commenters", 0),
        },
    }

    layers = [
        {
            "name": "Bronze",
            "status": "active",
            "description": "Dados brutos extraidos da API publica do Reddit",
            "tech": "Airflow + Python requests -> JSON + S3",
            "records": summary.get("total_posts", 0),
            "subreddits": summary.get("unique_subreddits", 0),
            "sample": None,
        },
        {
            "name": "Silver",
            "status": "active",
            "description": "Posts e comentarios limpos, deduplicados",
            "tech": "Databricks + PySpark + Delta Lake",
            "records": len(silver_posts),
            "sample": silver_posts[0] if silver_posts else None,
        },
        {
            "name": "Gold",
            "status": "active",
            "description": "Metricas por subreddit/semana, top commenters",
            "tech": "Databricks SQL Warehouse (Serverless)",
            "records": summary.get("total_posts", 0),
            "sample": {
                "top_subreddit": subreddit_rankings[0] if subreddit_rankings else None,
                "top_commenter": top_commenters[0] if top_commenters else None,
                "summary": gold["summary"],
            },
        },
    ]

    snapshot = {
        "layers": layers,
        "silver_posts": silver_posts,
        "gold": gold,
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\nSnapshot salvo em {out_path} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
