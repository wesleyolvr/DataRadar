"""Testes das funções puras de extração do Reddit."""

from extract_reddit import _extract_parent_id, _flatten_comment_tree, _parse_post


class TestParsePost:
    def test_parses_all_fields(self, sample_raw_post):
        result = _parse_post(sample_raw_post)

        assert result["id"] == "abc123"
        assert result["subreddit"] == "dataengineering"
        assert result["title"] == "Migrating from Airflow to Dagster"
        assert result["author"] == "data_dev_42"
        assert result["score"] == 142
        assert result["upvote_ratio"] == 0.93
        assert result["num_comments"] == 47
        assert result["flair"] == "Discussion"
        assert result["is_self"] is True
        assert result["created_utc"] == 1711800000
        assert result["created_date"] is not None
        assert result["extracted_at"] is not None

    def test_handles_missing_optional_fields(self):
        raw = {"data": {"id": "minimal"}}
        result = _parse_post(raw)

        assert result["id"] == "minimal"
        assert result["selftext"] == ""
        assert result["score"] == 0
        assert result["num_comments"] == 0
        assert result["flair"] is None

    def test_handles_empty_data(self):
        raw = {}
        result = _parse_post(raw)

        assert result["id"] is None
        assert result["created_date"] is None

    def test_created_date_is_none_when_utc_is_zero(self):
        raw = {"data": {"id": "x", "created_utc": 0}}
        result = _parse_post(raw)

        assert result["created_date"] is None


class TestExtractParentId:
    def test_extracts_from_t1_prefix(self):
        assert _extract_parent_id("t1_abc123") == "abc123"

    def test_extracts_from_t3_prefix(self):
        assert _extract_parent_id("t3_post_id") == "post_id"

    def test_returns_none_for_empty_string(self):
        assert _extract_parent_id("") is None

    def test_returns_raw_when_no_underscore(self):
        assert _extract_parent_id("nounderscore") == "nounderscore"


class TestFlattenCommentTree:
    def test_flattens_single_comment(self, sample_raw_comment):
        result = _flatten_comment_tree([sample_raw_comment], post_id="abc123")

        assert len(result) == 1
        assert result[0]["id"] == "comment_1"
        assert result[0]["post_id"] == "abc123"
        assert result[0]["depth"] == 0

    def test_flattens_nested_replies(self):
        reply = {
            "kind": "t1",
            "data": {
                "id": "reply_1",
                "parent_id": "t1_comment_1",
                "author": "replier",
                "body": "I agree!",
                "score": 5,
                "created_utc": 1711807200,
                "replies": "",
            },
        }
        parent = {
            "kind": "t1",
            "data": {
                "id": "comment_1",
                "parent_id": "t3_abc123",
                "author": "commenter",
                "body": "Great post",
                "score": 10,
                "created_utc": 1711803600,
                "replies": {
                    "data": {"children": [reply]},
                },
            },
        }

        result = _flatten_comment_tree([parent], post_id="abc123")

        assert len(result) == 2
        assert result[0]["id"] == "comment_1"
        assert result[0]["depth"] == 0
        assert result[1]["id"] == "reply_1"
        assert result[1]["depth"] == 1

    def test_ignores_non_t1_kinds(self):
        more_item = {"kind": "more", "data": {"id": "more_1"}}
        result = _flatten_comment_tree([more_item], post_id="abc123")

        assert len(result) == 0

    def test_handles_empty_children(self):
        result = _flatten_comment_tree([], post_id="abc123")

        assert result == []
