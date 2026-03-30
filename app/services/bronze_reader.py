"""Leitura dos JSONs Bronze do filesystem local.

Suporta múltiplos snapshots por dia:
  - raw_*.json      → posts
  - comments_*.json → comentários
Deduplica por id mantendo o snapshot mais recente.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path
from typing import Any

DATA_DIR = Path(
    os.getenv("DEVRADAR_DATA_DIR", str(Path(__file__).resolve().parents[2] / "airflow" / "data"))
)


def _reddit_dir() -> Path:
    return DATA_DIR / "reddit"


def _find_snapshots(date_dir: Path) -> list[Path]:
    """Encontra todos os snapshots numa pasta de data (raw_*.json)."""
    if not date_dir.exists():
        return []
    return sorted(date_dir.glob("raw_*.json"))


def _load_and_dedupe(date_dir: Path) -> tuple[list[dict[str, Any]], int, str | None]:
    """Carrega todos os snapshots, deduplica por id (mais recente vence).

    Returns: (posts_dedupados, total_snapshots, último_snapshot_at)
    """
    snapshots = _find_snapshots(date_dir)
    if not snapshots:
        return [], 0, None

    seen: dict[str, dict[str, Any]] = {}
    last_snapshot_at: str | None = None

    for path in snapshots:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        snap_time = data.get("snapshot_at") or data.get("saved_at")
        if snap_time:
            last_snapshot_at = snap_time

        for post in data.get("posts", []):
            pid = post.get("id")
            if pid:
                seen[pid] = post

    return list(seen.values()), len(snapshots), last_snapshot_at


def list_subreddits() -> list[dict[str, Any]]:
    """Retorna subreddits disponíveis com datas, contagem e snapshots."""
    root = _reddit_dir()
    if not root.exists():
        return []

    results: list[dict[str, Any]] = []
    for sub_dir in sorted(root.iterdir()):
        if not sub_dir.is_dir():
            continue
        dates: list[dict[str, Any]] = []
        for date_dir in sorted(sub_dir.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue
            posts, num_snapshots, _ = _load_and_dedupe(date_dir)
            if not posts and num_snapshots == 0:
                continue
            comments, comment_snapshots, _ = _load_and_dedupe_comments(date_dir)
            date_str = date_dir.name.replace("date=", "")
            dates.append({
                "date": date_str,
                "count": len(posts),
                "snapshots": num_snapshots,
                "comments_count": len(comments),
                "comment_snapshots": comment_snapshots,
            })

        if dates:
            total = sum(d["count"] for d in dates)
            results.append({
                "subreddit": sub_dir.name,
                "dates": dates,
                "total_posts": total,
            })
    return results


def get_posts(
    subreddit: str,
    date: str,
    page: int = 1,
    per_page: int = 20,
    sort_by: str = "score",
) -> dict[str, Any]:
    """Retorna posts paginados (deduplicados) de um subreddit/data."""
    date_dir = _reddit_dir() / subreddit / f"date={date}"
    posts, num_snapshots, _ = _load_and_dedupe(date_dir)

    if not posts:
        return {"error": "not_found", "posts": [], "total": 0}

    reverse = True
    if sort_by == "date":
        posts.sort(key=lambda p: p.get("created_utc", 0), reverse=reverse)
    elif sort_by == "comments":
        posts.sort(key=lambda p: p.get("num_comments", 0), reverse=reverse)
    else:
        posts.sort(key=lambda p: p.get("score", 0), reverse=reverse)

    total = len(posts)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "subreddit": subreddit,
        "date": date,
        "total": total,
        "snapshots": num_snapshots,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "posts": posts[start:end],
    }


def get_stats(subreddit: str, date: str) -> dict[str, Any]:
    """Estatísticas de um subreddit/data (deduplicadas)."""
    date_dir = _reddit_dir() / subreddit / f"date={date}"
    posts, num_snapshots, last_at = _load_and_dedupe(date_dir)

    if not posts:
        return {"error": "not_found"}

    scores = [p.get("score", 0) for p in posts]
    comments = [p.get("num_comments", 0) for p in posts]
    flairs = Counter(p.get("flair") or "Sem flair" for p in posts)

    top_posts = sorted(posts, key=lambda p: p.get("score", 0), reverse=True)[:5]

    return {
        "subreddit": subreddit,
        "date": date,
        "total": len(posts),
        "snapshots": num_snapshots,
        "score_avg": round(sum(scores) / len(scores), 1),
        "score_max": max(scores),
        "score_min": min(scores),
        "comments_avg": round(sum(comments) / len(comments), 1),
        "comments_total": sum(comments),
        "flairs": dict(flairs.most_common(10)),
        "top_posts": [
            {"title": p["title"], "score": p["score"], "num_comments": p["num_comments"], "author": p.get("author")}
            for p in top_posts
        ],
        "last_snapshot_at": last_at,
    }


def get_all_posts_flat(limit: int = 500) -> list[dict[str, Any]]:
    """Retorna todos os posts Bronze deduplicados de todos os subreddits."""
    seen: dict[str, dict[str, Any]] = {}
    root = _reddit_dir()
    if not root.exists():
        return []

    for raw_path in sorted(root.glob("*/date=*/raw_*.json")):
        try:
            data = json.loads(raw_path.read_text(encoding="utf-8"))
            for post in data.get("posts", []):
                pid = post.get("id")
                if pid:
                    seen[pid] = post
        except (json.JSONDecodeError, OSError):
            continue

    all_posts = list(seen.values())
    all_posts.sort(key=lambda p: p.get("score", 0), reverse=True)
    return all_posts[:limit]


# ---------------------------------------------------------------------------
# Comentários
# ---------------------------------------------------------------------------

def _find_comment_snapshots(date_dir: Path) -> list[Path]:
    """Encontra todos os snapshots de comentários numa pasta de data."""
    if not date_dir.exists():
        return []
    return sorted(date_dir.glob("comments_*.json"))


def _load_and_dedupe_comments(
    date_dir: Path,
) -> tuple[list[dict[str, Any]], int, str | None]:
    """Carrega comentários de todos os snapshots, deduplica por id."""
    snapshots = _find_comment_snapshots(date_dir)
    if not snapshots:
        return [], 0, None

    seen: dict[str, dict[str, Any]] = {}
    last_snapshot_at: str | None = None

    for path in snapshots:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        snap_time = data.get("snapshot_at")
        if snap_time:
            last_snapshot_at = snap_time

        for comment in data.get("comments", []):
            cid = comment.get("id")
            if cid:
                seen[cid] = comment

    return list(seen.values()), len(snapshots), last_snapshot_at


def get_comments(
    subreddit: str,
    date: str,
    post_id: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_by: str = "score",
) -> dict[str, Any]:
    """Retorna comentários paginados (deduplicados) de um subreddit/data."""
    date_dir = _reddit_dir() / subreddit / f"date={date}"
    comments, num_snapshots, _ = _load_and_dedupe_comments(date_dir)

    if not comments:
        return {"error": "not_found", "comments": [], "total": 0}

    if post_id:
        comments = [c for c in comments if c.get("post_id") == post_id]

    if sort_by == "date":
        comments.sort(key=lambda c: c.get("created_utc", 0), reverse=True)
    else:
        comments.sort(key=lambda c: c.get("score", 0), reverse=True)

    total = len(comments)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "subreddit": subreddit,
        "date": date,
        "post_id": post_id,
        "total": total,
        "snapshots": num_snapshots,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "comments": comments[start:end],
    }


def get_comment_stats(subreddit: str, date: str) -> dict[str, Any]:
    """Estatísticas de comentários de um subreddit/data."""
    date_dir = _reddit_dir() / subreddit / f"date={date}"
    comments, num_snapshots, last_at = _load_and_dedupe_comments(date_dir)

    if not comments:
        return {"has_comments": False, "total": 0}

    scores = [c.get("score", 0) for c in comments]
    unique_posts = {c.get("post_id") for c in comments}
    unique_authors = {c.get("author") for c in comments if c.get("author")}
    depths = [c.get("depth", 0) for c in comments]

    top_comments = sorted(comments, key=lambda c: c.get("score", 0), reverse=True)[:5]

    return {
        "has_comments": True,
        "subreddit": subreddit,
        "date": date,
        "total": len(comments),
        "snapshots": num_snapshots,
        "unique_posts_with_comments": len(unique_posts),
        "unique_authors": len(unique_authors),
        "score_avg": round(sum(scores) / len(scores), 1) if scores else 0,
        "score_max": max(scores) if scores else 0,
        "avg_depth": round(sum(depths) / len(depths), 1) if depths else 0,
        "max_depth": max(depths) if depths else 0,
        "top_comments": [
            {
                "body": c["body"][:200],
                "score": c["score"],
                "author": c.get("author"),
                "post_id": c.get("post_id"),
                "depth": c.get("depth", 0),
            }
            for c in top_comments
        ],
        "last_snapshot_at": last_at,
    }
