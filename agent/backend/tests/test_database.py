from unittest.mock import MagicMock, patch

import pytest

from database import FORBIDDEN_KEYWORDS, MAX_ROWS_RETURNED, execute_sql_query


def _mock_result(rows=None, columns=None):
    result = MagicMock()
    result.result_rows = rows if rows is not None else []
    result.column_names = columns if columns is not None else []
    return result


def _run_with_mocked_client(query, rows=None, columns=None, side_effect=None):
    with patch("database._get_client") as mock_get_client:
        if side_effect is not None:
            mock_get_client.return_value.query.side_effect = side_effect
        else:
            mock_get_client.return_value.query.return_value = _mock_result(rows, columns)
        return execute_sql_query(query)


class TestStatementCount:
    def test_blocks_multiple_statements(self):
        result = execute_sql_query("SELECT 1; SELECT 2")
        assert "Only a single SELECT statement" in result

    def test_blocks_empty_query(self):
        result = execute_sql_query("   ")
        assert "Only a single SELECT statement" in result

    def test_allows_single_statement_with_trailing_semicolon(self):
        result = _run_with_mocked_client("SELECT 1;")
        assert "No data found" in result


class TestStatementType:
    def test_blocks_non_select_non_with(self):
        result = execute_sql_query("EXPLAIN SELECT 1")
        assert "Security block" in result

    def test_allows_with_select(self):
        result = _run_with_mocked_client("WITH x AS (SELECT 1) SELECT * FROM x")
        assert "No data found" in result

    def test_case_insensitive_select_check(self):
        result = _run_with_mocked_client("select 1")
        assert "No data found" in result


class TestForbiddenKeywords:
    @pytest.mark.parametrize("keyword", FORBIDDEN_KEYWORDS)
    def test_blocks_forbidden_keyword_in_query(self, keyword):
        result = execute_sql_query(
            f"SELECT * FROM gold.fact_trips WHERE 1=1 {keyword} something"
        )
        assert "Security block" in result

    def test_blocks_with_delete(self):
        # The exact attack shape the review calls out: WITH ... DELETE hidden
        # inside a CTE, even though the statement still starts with WITH.
        result = execute_sql_query(
            "WITH deleted AS (DELETE FROM gold.fact_trips RETURNING *) "
            "SELECT * FROM deleted"
        )
        assert "Security block" in result

    def test_blocks_lowercase_forbidden_keyword(self):
        result = execute_sql_query(
            "select * from gold.fact_trips; drop table gold.fact_trips"
        )
        assert "Only a single SELECT statement" in result

    def test_does_not_false_positive_on_substring_matches(self):
        # Identifiers that merely *contain* a forbidden keyword as a substring
        # (inserted_at, updated_by, system_id, created_at) must not be blocked —
        # the check is word-boundary based, not a naive substring search.
        result = _run_with_mocked_client(
            "SELECT inserted_at, updated_by, system_id, created_at "
            "FROM gold.fact_trips",
            rows=[(1, 2, 3, 4)],
            columns=["inserted_at", "updated_by", "system_id", "created_at"],
        )
        assert "Security block" not in result


class TestResultHandling:
    def test_returns_no_data_message_when_empty(self):
        result = _run_with_mocked_client("SELECT * FROM gold.fact_trips WHERE 1=0")
        assert result == "No data found for this query."

    def test_truncates_results_over_max_rows(self):
        rows = [(i,) for i in range(MAX_ROWS_RETURNED + 10)]
        result = _run_with_mocked_client(
            "SELECT x FROM gold.fact_trips", rows=rows, columns=["x"]
        )
        assert f"truncated to first {MAX_ROWS_RETURNED}" in result

    def test_does_not_truncate_results_under_max_rows(self):
        rows = [(i,) for i in range(5)]
        result = _run_with_mocked_client(
            "SELECT x FROM gold.fact_trips", rows=rows, columns=["x"]
        )
        assert "truncated" not in result

    def test_returns_database_error_message_on_exception(self):
        result = _run_with_mocked_client(
            "SELECT 1", side_effect=Exception("connection refused")
        )
        assert result == "Database Error: connection refused"
