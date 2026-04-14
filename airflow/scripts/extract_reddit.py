"""
DataRadar — Módulo de extração da API pública do Reddit.

Extrai posts e comentários. Retorna dados em memória (lista de dicts)
para que o caller decida onde persistir (S3, disco, etc.).
Nenhum I/O local é feito aqui.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)


def _default_headers() -> dict[str, str]:
    """Reddit exige User-Agent único e descritivo (não imitar browser — isso gera 403).

    Formato recomendado: ``platform:app:version (by /u/username)``.
    Ver: https://github.com/reddit-archive/reddit/wiki/API
    """
    user = os.getenv("REDDIT_USERNAME", "DataRadarBot").strip() or "DataRadarBot"
    return {
        "User-Agent": f"python:DataRadar:1.0 (by /u/{user})",
        "Accept": "application/json",
    }

REQUEST_TIMEOUT = 15
RATE_LIMIT_SLEEP = 3.0
MAX_RETRIES = 6
RETRY_BACKOFF = 5
MIN_429_WAIT = 10


def _get_with_retry(url: str, params: dict, retries: int = MAX_RETRIES) -> dict | None:
    """GET com retry exponencial e tratamento de rate-limit (429).

    Respeita o header ``Retry-After`` quando presente; caso contrário usa
    backoff exponencial com piso de MIN_429_WAIT segundos para 429s.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                url,
                headers=_default_headers(),
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait = max(int(retry_after), MIN_429_WAIT)
                else:
                    wait = max(RETRY_BACKOFF * (2 ** (attempt - 1)), MIN_429_WAIT)
                logger.warning(
                    "Rate-limited (429). Tentativa %d/%d — aguardando %ds…",
                    attempt, retries, wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as exc:
            logger.error(
                "Tentativa %d/%d falhou para %s: %s",
                attempt, retries, url, exc,
            )
            if attempt < retries:
                time.sleep(RETRY_BACKOFF * attempt)

    logger.error("Todas as %d tentativas falharam para %s", retries, url)
    return None


def _parse_post(raw: dict[str, Any]) -> dict[str, Any]:
    """Normaliza um post cru da API para um dict limpo."""
    d = raw.get("data", {})
    created_utc = d.get("created_utc", 0)

    return {
        "id": d.get("id"),
        "subreddit": d.get("subreddit"),
        "title": d.get("title"),
        "selftext": d.get("selftext", ""),
        "author": d.get("author"),
        "score": d.get("score", 0),
        "upvote_ratio": d.get("upvote_ratio", 0),
        "num_comments": d.get("num_comments", 0),
        "created_utc": created_utc,
        "created_date": (
            datetime.fromtimestamp(created_utc, tz=UTC).isoformat()
            if created_utc
            else None
        ),
        "permalink": d.get("permalink"),
        "url": d.get("url"),
        "flair": d.get("link_flair_text"),
        "is_self": d.get("is_self", True),
        "extracted_at": datetime.now(tz=UTC).isoformat(),
    }


def extract_subreddit(
    subreddit: str,
    sort: str = "hot",
    max_pages: int = 3,
    per_page: int = 100,
) -> list[dict[str, Any]]:
    """
    Extrai posts de um subreddit com paginação.

    Parameters
    ----------
    subreddit : str
        Nome do subreddit (sem o prefixo ``r/``).
    sort : str
        Endpoint de ordenação (``hot``, ``new``, ``top``, ``rising``).
    max_pages : int
        Quantidade máxima de páginas a percorrer.
    per_page : int
        Posts por página (máx. 100 pela API).

    Returns
    -------
    list[dict]
        Lista de posts normalizados. Retorna lista vazia em caso de falha total.
    """
    base_url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
    all_posts: list[dict[str, Any]] = []
    after: str | None = None

    logger.info(
        "Iniciando extração de r/%s (sort=%s, max_pages=%d)",
        subreddit, sort, max_pages,
    )

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {"limit": per_page, "raw_json": 1}
        if after:
            params["after"] = after

        logger.info("r/%s — página %d/%d (after=%s)", subreddit, page, max_pages, after)

        data = _get_with_retry(base_url, params)
        if data is None or "data" not in data:
            logger.warning("Sem dados na página %d de r/%s. Encerrando.", page, subreddit)
            break

        children = data["data"].get("children", [])
        parsed = [_parse_post(c) for c in children]
        all_posts.extend(parsed)

        logger.info("r/%s — página %d: %d posts extraídos", subreddit, page, len(parsed))

        after = data["data"].get("after")
        if not after:
            logger.info("r/%s — sem cursor 'after'. Fim da paginação.", subreddit)
            break

        time.sleep(RATE_LIMIT_SLEEP)

    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for post in all_posts:
        pid = post["id"]
        if pid and pid not in seen:
            seen.add(pid)
            unique.append(post)

    logger.info(
        "r/%s concluído: %d posts brutos → %d únicos",
        subreddit, len(all_posts), len(unique),
    )
    return unique


# ---------------------------------------------------------------------------
# Extração de comentários
# ---------------------------------------------------------------------------

def _flatten_comment_tree(
    children: list[dict[str, Any]],
    post_id: str,
    depth: int = 0,
) -> list[dict[str, Any]]:
    """Achata a árvore recursiva de comentários em uma lista flat."""
    flat: list[dict[str, Any]] = []
    for child in children:
        if child.get("kind") != "t1":
            continue
        d = child.get("data", {})
        created_utc = d.get("created_utc", 0)
        flat.append({
            "id": d.get("id"),
            "post_id": post_id,
            "parent_id": _extract_parent_id(d.get("parent_id", "")),
            "author": d.get("author"),
            "body": d.get("body", ""),
            "score": d.get("score", 0),
            "depth": depth,
            "created_utc": created_utc,
            "created_date": (
                datetime.fromtimestamp(created_utc, tz=UTC).isoformat()
                if created_utc
                else None
            ),
            "extracted_at": datetime.now(tz=UTC).isoformat(),
        })
        replies = d.get("replies")
        if isinstance(replies, dict):
            nested = replies.get("data", {}).get("children", [])
            flat.extend(_flatten_comment_tree(nested, post_id, depth + 1))
    return flat


def _extract_parent_id(raw_parent: str) -> str | None:
    """Converte 't1_abc123' ou 't3_abc123' em 'abc123'."""
    if "_" in raw_parent:
        return raw_parent.split("_", 1)[1]
    return raw_parent or None


def extract_post_comments(
    subreddit: str,
    post_id: str,
    limit: int = 100,
    depth: int = 3,
    sort: str = "top",
) -> list[dict[str, Any]]:
    """Extrai comentários de um post específico."""
    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
    params = {"limit": limit, "depth": depth, "sort": sort, "raw_json": 1}

    data = _get_with_retry(url, params)
    if not data or not isinstance(data, list) or len(data) < 2:
        logger.warning("Sem comentários para r/%s/comments/%s", subreddit, post_id)
        return []

    children = data[1].get("data", {}).get("children", [])
    comments = _flatten_comment_tree(children, post_id)

    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for c in comments:
        cid = c.get("id")
        if cid and cid not in seen:
            seen.add(cid)
            unique.append(c)

    return unique


def extract_comments_for_posts(
    posts: list[dict[str, Any]],
    cache: dict[str, int] | None = None,
    min_comments: int = 5,
    top_k: int = 50,
    comment_limit: int = 100,
    comment_depth: int = 3,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int]]:
    """Extrai comentários dos posts mais relevantes, usando cache para pular os inalterados.

    Parameters
    ----------
    posts : list[dict]
        Posts já extraídos (com id, subreddit, num_comments, score).
    cache : dict | None
        Cache {post_id: last_num_comments}. None = sem cache (extrai tudo).
    min_comments : int
        Só busca comentários de posts com >= min_comments.
    top_k : int
        Máximo de posts para buscar comentários (ordenados por engajamento).
    comment_limit : int
        Máximo de comentários raiz por post (passado à API).
    comment_depth : int
        Profundidade máxima de replies (passado à API).

    Returns
    -------
    (comments, updated_cache, stats)
        comments: lista flat de todos os comentários extraídos
        updated_cache: cache atualizado com os novos num_comments
        stats: {"total_eligible", "skipped_cache", "fetched", "failed", "total_comments"}
    """
    if cache is None:
        cache = {}

    eligible = [p for p in posts if p.get("num_comments", 0) >= min_comments]
    eligible.sort(
        key=lambda p: p.get("score", 0) + p.get("num_comments", 0) * 2,
        reverse=True,
    )
    eligible = eligible[:top_k]

    stats = {
        "total_eligible": len(eligible),
        "skipped_cache": 0,
        "fetched": 0,
        "failed": 0,
        "total_comments": 0,
    }

    all_comments: list[dict[str, Any]] = []
    updated_cache = dict(cache)

    for i, post in enumerate(eligible, 1):
        pid = post["id"]
        subreddit = post["subreddit"]
        current_count = post.get("num_comments", 0)
        cached_count = cache.get(pid, -1)

        if cached_count == current_count:
            logger.debug(
                "[%d/%d] r/%s post %s: %d comentários (sem mudança, pulando)",
                i, len(eligible), subreddit, pid, current_count,
            )
            stats["skipped_cache"] += 1
            continue

        logger.info(
            "[%d/%d] r/%s post %s: %d→%d comentários (extraindo)",
            i, len(eligible), subreddit, pid, max(cached_count, 0), current_count,
        )

        comments = extract_post_comments(
            subreddit=subreddit,
            post_id=pid,
            limit=comment_limit,
            depth=comment_depth,
        )

        if comments:
            all_comments.extend(comments)
            stats["fetched"] += 1
            stats["total_comments"] += len(comments)
            updated_cache[pid] = current_count
        else:
            stats["failed"] += 1

        # Pausa mais longa entre posts para evitar 429 em cascata
        time.sleep(RATE_LIMIT_SLEEP * 2)

    logger.info(
        "Extração de comentários concluída: %d elegíveis, %d pulados (cache), "
        "%d extraídos, %d falharam → %d comentários total",
        stats["total_eligible"], stats["skipped_cache"],
        stats["fetched"], stats["failed"], stats["total_comments"],
    )
    return all_comments, updated_cache, stats
