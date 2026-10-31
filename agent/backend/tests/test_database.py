from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import database
from database import MAX_ROWS_RETURNED, execute_sql_query, validate_sql


@pytest.mark.parametrize('query', [
    '', 'SELECT 1; SELECT 2', 'DROP TABLE gold.fact_trips',
    'INSERT INTO gold.fact_trips SELECT 1',
    'WITH x AS (DELETE FROM gold.fact_trips RETURNING *) SELECT * FROM x',
    'SELECT * FROM silver.silver_trips', 'SELECT * FROM agent.memory',
    'SELECT * FROM system.tables', 'SELECT * FROM fact_trips',
    "SELECT * FROM url('http://example.com')",
    "SELECT * FROM remote('example.com', 'gold', 'fact_trips')",
    "SELECT dictGet('secret', 'value', 1)",
    "SELECT * FROM gold.fact_trips INTO OUTFILE '/tmp/results'",
    'SELECT * FROM gold.fact_trips SETTINGS max_execution_time=0',
    'SELECT * FROM gold.fact_trips FORMAT CSV',
    'WITH fact_trips AS (SELECT * FROM agent.memory) SELECT * FROM fact_trips',
    'SELECT * FROM gold.fact_trips UNION ALL SELECT * FROM agent.memory',
    'WITH x AS (SELECT 1) SELECT * FROM silver.x',
])
def test_unsafe_query_never_reaches_database(query):
    with patch('database._get_client') as get_client:
        result = execute_sql_query(query)
    assert result.status == 'error'
    get_client.assert_not_called()


@pytest.mark.parametrize('query', [
    'SELECT 1;', 'select 1', "SELECT '; DROP TABLE' AS text",
    '/* comment */ SELECT inserted_at, updated_by FROM gold.fact_trips',
    'WITH x AS (SELECT * FROM gold.fact_trips) SELECT count() FROM x',
    'SELECT * FROM (SELECT * FROM marts.mart_city_flow) x',
    'SELECT count() FROM gold.fact_trips UNION ALL SELECT count() FROM marts.daily_ride_summary',
    'SELECT sum(ride_distance_sum) / nullIf(sum(ride_distance_count), 0) FROM marts.daily_ride_summary',
    "SELECT quantile(0.95)(ride_duration_minutes) FROM gold.fact_trips",
    "SELECT count() FROM gold.fact_trips WHERE toDate(started_at) BETWEEN '2024-10-05' AND '2024-10-06'",
    'SELECT toStartOfMonth(started_at), count() FROM gold.fact_trips GROUP BY 1',
    'SELECT count() FROM gold.fact_trips WHERE started_at >= now() - INTERVAL 30 DAY',
])
def test_valid_analytics_queries(query):
    validate_sql(query)


def test_cte_sources_are_resolved_not_guessed_from_names():
    _, sources = validate_sql('WITH x AS (SELECT * FROM gold.fact_trips) SELECT count() FROM x')
    assert sources == ['gold.fact_trips']


def test_result_limit_applies_on_server_and_reports_truncation():
    client = MagicMock()
    client.query.return_value = SimpleNamespace(
        result_rows=[(i,) for i in range(MAX_ROWS_RETURNED + 1)], column_names=['x'])
    with patch('database._get_client', return_value=client):
        result = execute_sql_query('SELECT ride_id FROM gold.fact_trips', timeout=4)
    assert result.status == 'ok' and result.truncated
    assert len(result.rows) == MAX_ROWS_RETURNED
    sql, = client.query.call_args.args
    settings = client.query.call_args.kwargs['settings']
    assert sql.endswith('LIMIT 51')
    assert settings['max_execution_time'] == 4
    assert settings['max_result_rows'] == 51
    assert settings['result_overflow_mode'] == 'throw'
    assert settings['max_memory_usage'] > 0
    client.close.assert_called_once()


def test_empty_and_failure_are_distinct_and_failure_hides_internal_details():
    client = MagicMock()
    client.query.return_value = SimpleNamespace(result_rows=[], column_names=['x'])
    with patch('database._get_client', return_value=client):
        assert execute_sql_query('SELECT 1 WHERE 0').status == 'empty'
        client.query.side_effect = RuntimeError('secret.internal.host credential')
        result = execute_sql_query('SELECT 1')
    assert result.status == 'error'
    assert 'secret.internal' not in result.error


def test_schema_refreshes_after_ttl_and_coverage_comes_from_facts(monkeypatch):
    monkeypatch.setattr(database, '_schema_context', None)
    monkeypatch.setattr(database, '_schema_expires_at', 0)
    clock = [0]
    monkeypatch.setattr(database, 'monotonic', lambda: clock[0])
    client = MagicMock()
    columns = [('gold', 'fact_trips', 'ride_id', 'String', 'Natural key')]
    bounds = [(date(2024, 10, 5), date(2024, 10, 6), 3)]
    client.query.side_effect = [SimpleNamespace(result_rows=rows)
                                for rows in [columns, [], [], [], [], bounds,
                                             columns + [('gold', 'fact_trips', 'new_column', 'Int32', '')],
                                             [], [], [], [], bounds]]
    with patch('database._get_client', return_value=client) as factory:
        first = database.get_schema_context()
        assert database.get_schema_context() is first
        factory.assert_called_once()
        clock[0] = 301
        second = database.get_schema_context()
    assert first.version != second.version
    assert 'new_column' in second.text
    assert '2024-10-06' in second.text
    assert any('FROM gold.fact_trips' in call.args[0] for call in client.query.call_args_list)


def test_memory_is_optional_without_credentials():
    with patch('database._get_client') as client:
        assert database.retrieve_memory('q', 'v') == []
        assert not database.save_memory('q', 'SELECT 1', 'v')
    client.assert_not_called()


def test_memory_threshold_version_and_deduplication(monkeypatch):
    monkeypatch.setenv('AI_MEMORY_PASSWORD', 'test-memory')
    memory = MagicMock()
    memory.embed_text.return_value = [0.1, 0.2]
    client = MagicMock()
    client.query.return_value = SimpleNamespace(result_rows=[('near', 'SELECT 1', 0.1),
                                                             ('far', 'SELECT 2', 0.9)])
    with patch.dict('sys.modules', memory=memory), patch('database._get_client', return_value=client) as factory:
        assert [row['question'] for row in database.retrieve_memory('q', 'schema-v2')] == ['near']
        factory.assert_called_with(memory=True)
        assert client.query.call_args.kwargs['parameters']['version'] == 'schema-v2'
        assert 'validated = 1' in client.query.call_args.args[0]
        assert 'GROUP BY question, query' in client.query.call_args.args[0]
        client.query.return_value = SimpleNamespace(result_rows=[(1,)])
        assert database.save_memory('q', 'SELECT 1', 'schema-v2')
        client.insert.assert_not_called()


def test_embedding_failure_does_not_break_chat(monkeypatch):
    monkeypatch.setenv('AI_MEMORY_PASSWORD', 'test-memory')
    memory = MagicMock()
    memory.embed_text.side_effect = RuntimeError('model unavailable')
    with patch.dict('sys.modules', memory=memory), patch('database._get_client'):
        assert database.retrieve_memory('q', 'v') == []
