"""Cliente para consultar tabelas Silver/Gold no Databricks via SQL Connector."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "default")


def is_configured() -> bool:
    return bool(
        os.getenv("DATABRICKS_HOST")
        and os.getenv("DATABRICKS_TOKEN")
        and os.getenv("DATABRICKS_WAREHOUSE_ID")
    )


def _get_connection():
    from databricks import sql as databricks_sql

    return databricks_sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST", ""),
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID', '')}",
        access_token=os.getenv("DATABRICKS_TOKEN", ""),
        catalog=os.getenv("DATABRICKS_CATALOG", "workspace"),
        schema=os.getenv("DATABRICKS_SCHEMA", "default"),
    )


def _execute_query(query: str) -> tuple[list[tuple], list[str]]:
    conn = _get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return rows, columns
    finally:
        conn.close()


def _rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row, strict=False)) for row in rows]


def fetch_silver_posts(limit: int = 30) -> list[dict[str, Any]]:
    query = f"""
        SELECT id, subreddit, title, selftext, author, score, upvote_ratio,
               num_comments, created_date, permalink, flair, is_self, ingest_date
        FROM devradar_silver_posts
        ORDER BY score DESC
        LIMIT {limit}
    """
    try:
        rows, columns = _execute_query(query)
        return _rows_to_dicts(rows, columns)
    except Exception as e:
        logger.error("Erro ao buscar Silver posts: %s", e)
        return []


def fetch_silver_comments(limit: int = 50) -> list[dict[str, Any]]:
    query = f"""
        SELECT id, post_id, subreddit, author, body, score, depth, created_date
        FROM devradar_silver_comments
        ORDER BY score DESC
        LIMIT {limit}
    """
    try:
        rows, columns = _execute_query(query)
        return _rows_to_dicts(rows, columns)
    except Exception as e:
        logger.error("Erro ao buscar Silver comments: %s", e)
        return []


def fetch_gold_subreddit_week(limit: int = 20) -> list[dict[str, Any]]:
    query = f"""
        SELECT week_start, subreddit, post_count, sum_score, sum_num_comments,
               avg_score, avg_comments_per_post, total_comments_extracted,
               avg_comment_score, unique_commenters
        FROM devradar_gold_subreddit_week
        ORDER BY post_count DESC
        LIMIT {limit}
    """
    try:
        rows, columns = _execute_query(query)
        return _rows_to_dicts(rows, columns)
    except Exception as e:
        logger.error("Erro ao buscar Gold subreddit week: %s", e)
        return []


def fetch_gold_top_commenters(limit: int = 20) -> list[dict[str, Any]]:
    query = f"""
        SELECT subreddit, author, comment_count, total_score, avg_score, posts_commented
        FROM devradar_gold_top_commenters
        ORDER BY comment_count DESC
        LIMIT {limit}
    """
    try:
        rows, columns = _execute_query(query)
        return _rows_to_dicts(rows, columns)
    except Exception as e:
        logger.error("Erro ao buscar Gold top commenters: %s", e)
        return []


def fetch_gold_summary() -> dict[str, Any]:
    query = """
        SELECT
            COUNT(DISTINCT subreddit) as unique_subreddits,
            SUM(post_count) as total_posts,
            ROUND(AVG(avg_score), 1) as avg_score,
            SUM(total_comments_extracted) as total_comments,
            SUM(unique_commenters) as total_unique_commenters
        FROM devradar_gold_subreddit_week
    """
    try:
        rows, columns = _execute_query(query)
        if rows:
            return dict(zip(columns, rows[0], strict=False))
        return {}
    except Exception as e:
        logger.error("Erro ao buscar Gold summary: %s", e)
        return {}
