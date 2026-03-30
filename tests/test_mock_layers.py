"""Testes das funções de transformação Silver/Gold (mock_layers)."""

from services.mock_layers import _extract_tools, aggregate_to_gold, transform_to_silver


class TestExtractTools:
    def test_finds_single_tool(self):
        assert _extract_tools("I love using Apache Spark for ETL") == ["Apache Spark"]

    def test_finds_multiple_tools(self):
        tools = _extract_tools("We use Airflow, dbt and Kafka in our stack")
        assert "Apache Airflow" in tools
        assert "Apache Kafka" in tools
        assert "dbt" in tools

    def test_case_insensitive(self):
        assert _extract_tools("SPARK is great") == ["Apache Spark"]

    def test_deduplicates_aliases(self):
        tools = _extract_tools("pyspark is just spark for python")
        assert tools.count("Apache Spark") == 1

    def test_no_tools_found(self):
        assert _extract_tools("This post has no tech mentions") == []

    def test_empty_string(self):
        assert _extract_tools("") == []


class TestTransformToSilver:
    def test_filters_deleted_authors(self):
        posts = [
            {"id": "1", "title": "Good post", "author": "[deleted]", "selftext": ""},
            {"id": "2", "title": "Real post", "author": "dev1", "selftext": "Uses Spark"},
        ]
        result = transform_to_silver(posts)

        assert len(result) == 1
        assert result[0]["id"] == "2"

    def test_filters_removed_authors(self):
        posts = [{"id": "1", "title": "Post", "author": "[removed]", "selftext": ""}]
        assert transform_to_silver(posts) == []

    def test_filters_none_authors(self):
        posts = [{"id": "1", "title": "Post", "author": None, "selftext": ""}]
        assert transform_to_silver(posts) == []

    def test_filters_no_title(self):
        posts = [{"id": "1", "title": "", "author": "dev", "selftext": "text"}]
        assert transform_to_silver(posts) == []

    def test_extracts_tools_from_title_and_body(self):
        posts = [
            {
                "id": "1",
                "title": "Spark vs Flink",
                "selftext": "We also use Kafka",
                "author": "dev",
                "score": 10,
                "num_comments": 5,
                "created_date": "2026-01-01",
                "flair": "Discussion",
                "subreddit": "dataengineering",
            },
        ]
        result = transform_to_silver(posts)

        assert len(result) == 1
        assert "Apache Spark" in result[0]["tools_mentioned"]
        assert "Apache Flink" in result[0]["tools_mentioned"]
        assert "Apache Kafka" in result[0]["tools_mentioned"]
        assert result[0]["tools_count"] == 3

    def test_truncates_selftext(self):
        posts = [
            {"id": "1", "title": "Post", "author": "dev", "selftext": "x" * 1000, "score": 0, "num_comments": 0, "created_date": None, "flair": None, "subreddit": "test"},
        ]
        result = transform_to_silver(posts)

        assert len(result[0]["selftext_clean"]) == 500

    def test_empty_input(self):
        assert transform_to_silver([]) == []


class TestAggregateToGold:
    def test_empty_input(self):
        result = aggregate_to_gold([])
        assert result["tool_rankings"] == []
        assert result["subreddit_rankings"] == []

    def test_counts_tools_and_subreddits(self):
        silver = [
            {"subreddit": "dataengineering", "score": 100, "tools_mentioned": ["Apache Spark", "dbt"]},
            {"subreddit": "dataengineering", "score": 50, "tools_mentioned": ["Apache Spark"]},
            {"subreddit": "python", "score": 80, "tools_mentioned": ["FastAPI"]},
        ]
        result = aggregate_to_gold(silver)

        tool_names = [t["tool"] for t in result["tool_rankings"]]
        assert "Apache Spark" in tool_names

        spark = next(t for t in result["tool_rankings"] if t["tool"] == "Apache Spark")
        assert spark["mentions"] == 2
        assert spark["total_score"] == 150

        assert result["summary"]["total_posts"] == 3
        assert result["summary"]["unique_subreddits"] == 2
