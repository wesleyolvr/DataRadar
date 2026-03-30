"""Fixtures compartilhadas para todos os testes."""

import json

import pytest


@pytest.fixture
def sample_raw_post():
    """Post cru no formato da API do Reddit."""
    return {
        "kind": "t3",
        "data": {
            "id": "abc123",
            "subreddit": "dataengineering",
            "title": "Migrating from Airflow to Dagster",
            "selftext": "We've been using Apache Airflow for 2 years and considering Dagster...",
            "author": "data_dev_42",
            "score": 142,
            "upvote_ratio": 0.93,
            "num_comments": 47,
            "created_utc": 1711800000,
            "permalink": "/r/dataengineering/comments/abc123/migrating_from_airflow_to_dagster/",
            "url": "https://www.reddit.com/r/dataengineering/comments/abc123/",
            "link_flair_text": "Discussion",
            "is_self": True,
        },
    }


@pytest.fixture
def sample_raw_comment():
    """Comentário cru no formato da API do Reddit."""
    return {
        "kind": "t1",
        "data": {
            "id": "comment_1",
            "parent_id": "t3_abc123",
            "author": "spark_fan",
            "body": "We switched to Dagster last year. The asset-based approach is great.",
            "score": 28,
            "created_utc": 1711803600,
            "replies": "",
        },
    }


@pytest.fixture
def sample_parsed_post():
    """Post já normalizado (output de _parse_post)."""
    return {
        "id": "abc123",
        "subreddit": "dataengineering",
        "title": "Migrating from Airflow to Dagster",
        "selftext": "We've been using Apache Airflow for 2 years and considering Dagster...",
        "author": "data_dev_42",
        "score": 142,
        "upvote_ratio": 0.93,
        "num_comments": 47,
        "created_utc": 1711800000,
        "created_date": "2024-03-30T12:00:00+00:00",
        "permalink": "/r/dataengineering/comments/abc123/migrating_from_airflow_to_dagster/",
        "url": "https://www.reddit.com/r/dataengineering/comments/abc123/",
        "flair": "Discussion",
        "is_self": True,
        "extracted_at": "2024-03-30T12:05:00+00:00",
    }


@pytest.fixture
def sample_bronze_snapshot(tmp_path):
    """Cria um snapshot Bronze válido em disco e retorna o diretório."""
    date_dir = tmp_path / "reddit" / "dataengineering" / "date=2026-03-30"
    date_dir.mkdir(parents=True)

    payload = {
        "subreddit": "dataengineering",
        "execution_date": "2026-03-30",
        "snapshot_at": "2026-03-30T12:00:00",
        "count": 3,
        "posts": [
            {"id": "p1", "subreddit": "dataengineering", "title": "Post about Spark", "score": 100, "num_comments": 20, "created_utc": 1711800000, "author": "dev1", "flair": "Discussion"},
            {"id": "p2", "subreddit": "dataengineering", "title": "Post about dbt", "score": 50, "num_comments": 10, "created_utc": 1711800100, "author": "dev2", "flair": "Tutorial"},
            {"id": "p3", "subreddit": "dataengineering", "title": "Post about Kafka", "score": 75, "num_comments": 15, "created_utc": 1711800200, "author": "dev3", "flair": None},
        ],
    }

    snapshot_path = date_dir / "raw_2026-03-30T12_00_00.json"
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    return tmp_path


@pytest.fixture
def sample_bronze_with_dupes(tmp_path):
    """Dois snapshots com posts duplicados (IDs repetidos, scores diferentes)."""
    date_dir = tmp_path / "reddit" / "dataengineering" / "date=2026-03-30"
    date_dir.mkdir(parents=True)

    snap1 = {
        "subreddit": "dataengineering",
        "snapshot_at": "2026-03-30T10:00:00",
        "count": 2,
        "posts": [
            {"id": "p1", "title": "Spark old", "score": 50, "num_comments": 10, "created_utc": 1711800000, "subreddit": "dataengineering", "author": "dev1"},
            {"id": "p2", "title": "dbt old", "score": 30, "num_comments": 5, "created_utc": 1711800100, "subreddit": "dataengineering", "author": "dev2"},
        ],
    }

    snap2 = {
        "subreddit": "dataengineering",
        "snapshot_at": "2026-03-30T14:00:00",
        "count": 2,
        "posts": [
            {"id": "p1", "title": "Spark updated", "score": 150, "num_comments": 40, "created_utc": 1711800000, "subreddit": "dataengineering", "author": "dev1"},
            {"id": "p3", "title": "Kafka new", "score": 75, "num_comments": 15, "created_utc": 1711800200, "subreddit": "dataengineering", "author": "dev3"},
        ],
    }

    (date_dir / "raw_2026-03-30T10_00_00.json").write_text(json.dumps(snap1), encoding="utf-8")
    (date_dir / "raw_2026-03-30T14_00_00.json").write_text(json.dumps(snap2), encoding="utf-8")

    return tmp_path


@pytest.fixture
def sample_bronze_comments(tmp_path):
    """Snapshot de comentários Bronze para testes de list_subreddits."""
    date_dir = tmp_path / "reddit" / "dataengineering" / "date=2026-03-30"
    date_dir.mkdir(parents=True, exist_ok=True)

    comments_payload = {
        "subreddit": "dataengineering",
        "snapshot_at": "2026-03-30T12:00:00",
        "count": 2,
        "comments": [
            {"id": "c1", "post_id": "p1", "author": "user1", "body": "Great post!", "score": 10, "depth": 0, "created_utc": 1711803600},
            {"id": "c2", "post_id": "p1", "author": "user2", "body": "Agree!", "score": 5, "depth": 1, "created_utc": 1711807200},
        ],
    }

    (date_dir / "comments_2026-03-30T12_00_00.json").write_text(json.dumps(comments_payload), encoding="utf-8")

    posts_payload = {
        "subreddit": "dataengineering",
        "snapshot_at": "2026-03-30T12:00:00",
        "count": 1,
        "posts": [
            {"id": "p1", "subreddit": "dataengineering", "title": "Test", "score": 100, "num_comments": 20, "created_utc": 1711800000, "author": "dev1", "flair": "Discussion"},
        ],
    }

    (date_dir / "raw_2026-03-30T12_00_00.json").write_text(json.dumps(posts_payload), encoding="utf-8")

    return tmp_path
