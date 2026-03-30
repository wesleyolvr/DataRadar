"""Testes de leitura e deduplicação Bronze (filesystem)."""

import json
from unittest.mock import patch

from services.bronze_reader import (
    _load_and_dedupe,
    _load_and_dedupe_comments,
    get_posts,
    get_stats,
    list_subreddits,
)


class TestLoadAndDedupe:
    def test_loads_single_snapshot(self, sample_bronze_snapshot):
        date_dir = sample_bronze_snapshot / "reddit" / "dataengineering" / "date=2026-03-30"
        posts, num_snapshots, last_at = _load_and_dedupe(date_dir)

        assert num_snapshots == 1
        assert len(posts) == 3
        assert last_at == "2026-03-30T12:00:00"

    def test_deduplicates_by_id_keeping_latest(self, sample_bronze_with_dupes):
        date_dir = sample_bronze_with_dupes / "reddit" / "dataengineering" / "date=2026-03-30"
        posts, num_snapshots, _ = _load_and_dedupe(date_dir)

        assert num_snapshots == 2
        assert len(posts) == 3

        p1 = next(p for p in posts if p["id"] == "p1")
        assert p1["title"] == "Spark updated"
        assert p1["score"] == 150

    def test_empty_directory(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        posts, num_snapshots, last_at = _load_and_dedupe(empty_dir)

        assert posts == []
        assert num_snapshots == 0
        assert last_at is None

    def test_nonexistent_directory(self, tmp_path):
        posts, num_snapshots, last_at = _load_and_dedupe(tmp_path / "nope")

        assert posts == []
        assert num_snapshots == 0

    def test_handles_corrupted_json(self, tmp_path):
        date_dir = tmp_path / "reddit" / "test" / "date=2026-03-30"
        date_dir.mkdir(parents=True)
        (date_dir / "raw_corrupt.json").write_text("{invalid json", encoding="utf-8")
        (date_dir / "raw_valid.json").write_text(
            json.dumps({"posts": [{"id": "ok", "title": "Valid"}], "snapshot_at": "2026-03-30T12:00:00"}),
            encoding="utf-8",
        )

        posts, num_snapshots, _ = _load_and_dedupe(date_dir)

        assert num_snapshots == 2
        assert len(posts) == 1
        assert posts[0]["id"] == "ok"


class TestLoadAndDedupeComments:
    def test_loads_comments(self, sample_bronze_comments):
        date_dir = sample_bronze_comments / "reddit" / "dataengineering" / "date=2026-03-30"
        comments, num_snapshots, last_at = _load_and_dedupe_comments(date_dir)

        assert num_snapshots == 1
        assert len(comments) == 2
        assert last_at == "2026-03-30T12:00:00"

    def test_empty_when_no_comments(self, tmp_path):
        date_dir = tmp_path / "empty"
        date_dir.mkdir()
        comments, num_snapshots, last_at = _load_and_dedupe_comments(date_dir)

        assert comments == []
        assert num_snapshots == 0


class TestGetPosts:
    def test_returns_paginated_posts(self, sample_bronze_snapshot):
        with patch("services.bronze_reader._reddit_dir", return_value=sample_bronze_snapshot / "reddit"):
            result = get_posts("dataengineering", "2026-03-30", page=1, per_page=2)

        assert result["total"] == 3
        assert result["pages"] == 2
        assert len(result["posts"]) == 2

    def test_sorts_by_score_by_default(self, sample_bronze_snapshot):
        with patch("services.bronze_reader._reddit_dir", return_value=sample_bronze_snapshot / "reddit"):
            result = get_posts("dataengineering", "2026-03-30", page=1, per_page=10)

        scores = [p["score"] for p in result["posts"]]
        assert scores == sorted(scores, reverse=True)

    def test_returns_empty_for_missing_subreddit(self, tmp_path):
        with patch("services.bronze_reader._reddit_dir", return_value=tmp_path / "reddit"):
            result = get_posts("nonexistent", "2026-03-30")

        assert result["total"] == 0
        assert result["posts"] == []

    def test_page_2(self, sample_bronze_snapshot):
        with patch("services.bronze_reader._reddit_dir", return_value=sample_bronze_snapshot / "reddit"):
            result = get_posts("dataengineering", "2026-03-30", page=2, per_page=2)

        assert len(result["posts"]) == 1
        assert result["page"] == 2


class TestGetStats:
    def test_returns_stats(self, sample_bronze_snapshot):
        with patch("services.bronze_reader._reddit_dir", return_value=sample_bronze_snapshot / "reddit"):
            result = get_stats("dataengineering", "2026-03-30")

        assert result["total"] == 3
        assert "score_avg" in result
        assert "score_max" in result
        assert "top_posts" in result
        assert len(result["top_posts"]) <= 5

    def test_returns_error_for_missing(self, tmp_path):
        with patch("services.bronze_reader._reddit_dir", return_value=tmp_path / "reddit"):
            result = get_stats("nonexistent", "2026-03-30")

        assert result.get("error") == "not_found"


class TestListSubreddits:
    def test_lists_available_subreddits(self, sample_bronze_comments):
        with patch("services.bronze_reader._reddit_dir", return_value=sample_bronze_comments / "reddit"):
            result = list_subreddits()

        assert len(result) == 1
        assert result[0]["subreddit"] == "dataengineering"
        assert result[0]["total_posts"] == 1

    def test_returns_empty_when_no_data(self, tmp_path):
        with patch("services.bronze_reader._reddit_dir", return_value=tmp_path / "reddit"):
            assert list_subreddits() == []
