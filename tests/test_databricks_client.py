"""Testes do cliente Databricks — sem conexao real, testa parsing e config check."""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))


class TestIsConfigured:
    def test_returns_false_without_env(self):
        with patch.dict(os.environ, {}, clear=True):
            from services.databricks_client import is_configured
            assert not is_configured()

    def test_returns_false_with_partial_env(self):
        env = {"DATABRICKS_HOST": "dbc-xxx.cloud.databricks.com"}
        with patch.dict(os.environ, env, clear=True):
            from services.databricks_client import is_configured
            assert not is_configured()

    def test_returns_true_with_all_env(self):
        env = {
            "DATABRICKS_HOST": "dbc-xxx.cloud.databricks.com",
            "DATABRICKS_TOKEN": "dapi123",
            "DATABRICKS_WAREHOUSE_ID": "abc123",
        }
        with patch.dict(os.environ, env, clear=True):
            from services.databricks_client import is_configured
            assert is_configured()


class TestRowsToDicts:
    def test_converts_rows_to_dicts(self):
        from services.databricks_client import _rows_to_dicts

        rows = [
            ("id1", "python", "Post about Spark", 42),
            ("id2", "dataengineering", "Airflow tips", 15),
        ]
        columns = ["id", "subreddit", "title", "score"]

        result = _rows_to_dicts(rows, columns)

        assert len(result) == 2
        assert result[0] == {"id": "id1", "subreddit": "python", "title": "Post about Spark", "score": 42}
        assert result[1] == {"id": "id2", "subreddit": "dataengineering", "title": "Airflow tips", "score": 15}

    def test_empty_rows(self):
        from services.databricks_client import _rows_to_dicts

        result = _rows_to_dicts([], ["id", "title"])
        assert result == []

    def test_single_column(self):
        from services.databricks_client import _rows_to_dicts

        rows = [("value1",), ("value2",)]
        columns = ["col"]

        result = _rows_to_dicts(rows, columns)
        assert result == [{"col": "value1"}, {"col": "value2"}]
