"""Endpoints da camada Bronze — dados reais do filesystem."""

from __future__ import annotations

from fastapi import APIRouter, Query
from services.bronze_reader import (
    get_comment_stats,
    get_comments,
    get_posts,
    get_stats,
    list_subreddits,
)

router = APIRouter(prefix="/api/v1/bronze", tags=["bronze"])


@router.get("/subreddits")
def subreddits():
    return list_subreddits()


@router.get("/{subreddit}/{date}")
def posts(
    subreddit: str,
    date: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    sort_by: str = Query("score", pattern="^(score|comments|date)$"),
):
    return get_posts(subreddit, date, page=page, per_page=per_page, sort_by=sort_by)


@router.get("/{subreddit}/{date}/stats")
def stats(subreddit: str, date: str):
    return get_stats(subreddit, date)


@router.get("/{subreddit}/{date}/comments")
def comments(
    subreddit: str,
    date: str,
    post_id: str | None = Query(None, description="Filtrar por post_id"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort_by: str = Query("score", pattern="^(score|date)$"),
):
    return get_comments(
        subreddit, date, post_id=post_id,
        page=page, per_page=per_page, sort_by=sort_by,
    )


@router.get("/{subreddit}/{date}/comments/stats")
def comment_stats(subreddit: str, date: str):
    return get_comment_stats(subreddit, date)
