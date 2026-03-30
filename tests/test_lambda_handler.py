"""Testes das funções de validação do Lambda handler."""

from handler import _is_valid_path, _should_process


class TestShouldProcess:
    def test_accepts_raw_json(self):
        assert _should_process("reddit/python/date=2026-03-30/raw_2026-03-30T12_00_00.json") is True

    def test_rejects_comments_json(self):
        assert _should_process("reddit/python/date=2026-03-30/comments_2026-03-30T12_00_00.json") is False

    def test_rejects_cache_json(self):
        assert _should_process("reddit/python/posts_cache.json") is False

    def test_rejects_non_json(self):
        assert _should_process("reddit/python/date=2026-03-30/raw_data.csv") is False

    def test_accepts_raw_in_nested_path(self):
        assert _should_process("reddit/dataengineering/date=2026-01-15/raw_snapshot.json") is True

    def test_handles_flat_filename(self):
        assert _should_process("raw_test.json") is True

    def test_rejects_random_file(self):
        assert _should_process("readme.md") is False


class TestIsValidPath:
    def test_accepts_valid_path(self):
        assert _is_valid_path("reddit/python/date=2026-03-30/raw_2026-03-30T12_00_00.json") is True

    def test_rejects_missing_date_prefix(self):
        assert _is_valid_path("reddit/python/2026-03-30/raw_data.json") is False

    def test_rejects_missing_subreddit(self):
        assert _is_valid_path("reddit/date=2026-03-30/raw_data.json") is False

    def test_rejects_wrong_prefix(self):
        assert _is_valid_path("other/python/date=2026-03-30/raw_data.json") is False

    def test_rejects_malformed_date(self):
        assert _is_valid_path("reddit/python/date=2026-3-30/raw_data.json") is False

    def test_accepts_comments_path(self):
        assert _is_valid_path("reddit/python/date=2026-03-30/comments_2026.json") is True
