"""Opt-in tests against a fresh, disposable ClickHouse 24.3 container only.

CLICKHOUSE_TEST_PORT and CLICKHOUSE_TEST_CONTAINER select the disposable server.
CREATE DATABASE deliberately fails if gold/marts/agent already exist: these tests
never truncate or overwrite an existing warehouse.
"""
import os
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import clickhouse_connect
import pytest
import sqlglot

from database import execute_sql_query, get_schema_context, retrieve_memory, save_memory
from evaluation import CASES, DATA, MODEL_NAMES, ROOT, TABLES, render_model, rows_match

pytestmark = pytest.mark.skipif(
    not os.getenv('CLICKHOUSE_TEST_PORT') or not os.getenv('CLICKHOUSE_TEST_CONTAINER'),
    reason='Requires an explicitly configured disposable ClickHouse container',
)


@pytest.fixture(scope='module')
def warehouse():
    port = int(os.environ['CLICKHOUSE_TEST_PORT'])
    root = None
    for _ in range(20):
        try:
            root = clickhouse_connect.get_client(host='127.0.0.1', port=port)
            break
        except Exception:
            time.sleep(0.25)
    assert root is not None, 'Disposable ClickHouse did not start'
    for database in ['gold', 'marts', 'agent']:
        root.command(f'CREATE DATABASE {database}')
    root.command("CREATE USER ai_agent IDENTIFIED BY 'integration-agent'")
    root.command('''CREATE TABLE agent.memory (
        question String, query String, embedding Array(Float32),
        created_at DateTime DEFAULT now()) ENGINE=MergeTree ORDER BY tuple()''')
    root.command('GRANT SELECT, INSERT ON agent.memory TO ai_agent')
    root.command('GRANT SELECT ON gold.* TO ai_agent')
    root.command('GRANT SELECT ON marts.* TO ai_agent')
    root.command('GRANT SELECT ON system.columns TO ai_agent')
    root.command('GRANT SELECT ON system.tables TO ai_agent')
    container = os.environ['CLICKHOUSE_TEST_CONTAINER']
    subprocess.run(['docker', 'cp', str(ROOT / 'infra/clickhouse/agent-access.sh'),
                    f'{container}:/tmp/agent-access.sh'], check=True, capture_output=True)
    for _ in range(2):  # Migration is safe to re-run.
        subprocess.run(['docker', 'exec', '-e', 'CLICKHOUSE_NATIVE_PORT=9000',
                        '-e', 'AI_MEMORY_PASSWORD=integration-memory', container,
                        'bash', '/tmp/agent-access.sh'], check=True, capture_output=True)
    for table, columns in TABLES.items():
        ddl = sqlglot.transpile(f'CREATE TABLE {table} ({columns})', read='duckdb', write='clickhouse')[0]
        # Nullable metrics exercise non-null denominator handling.
        for col in ['ride_distance_km', 'temperature_c']:
            ddl = ddl.replace(f'{col} Float64', f'{col} Nullable(Float64)')
        for col in ['started_at', 'ended_at']:
            ddl = ddl.replace(f'{col} Nullable(DateTime)', f"{col} DateTime('America/New_York')")
        root.command(ddl + ' ENGINE=Memory')
    for table, rows in DATA.items():
        columns = root.query(f'DESCRIBE TABLE {table}').result_rows
        # Let ClickHouse parse the fixture's literal timestamps.
        if table == 'gold.fact_trips':
            from datetime import datetime
            from zoneinfo import ZoneInfo

            rows = [tuple(datetime.fromisoformat(v).replace(tzinfo=ZoneInfo('America/New_York')) if i in (2, 3) else v
                          for i, v in enumerate(row)) for row in rows]
        root.insert(table, rows, column_names=[col[0] for col in columns])
    root.command("""INSERT INTO gold.dim_date SELECT
        toYYYYMMDD(d), d, toDayOfWeek(d), toUInt8(toDayOfWeek(d) >= 6)
        FROM (SELECT toDate('2024-10-01') + number AS d FROM numbers(32))""")
    for name in MODEL_NAMES:
        root.command(f'CREATE TABLE marts.{name} ENGINE=Memory AS ' + render_model(name))
    yield root, port
    root.close()


@pytest.fixture
def agent_database(warehouse, monkeypatch):
    import database

    _, port = warehouse
    monkeypatch.setenv('CLICKHOUSE_HOST', '127.0.0.1')
    monkeypatch.setenv('CLICKHOUSE_HTTP_PORT', str(port))
    monkeypatch.setenv('AI_AGENT_PASSWORD', 'integration-agent')
    monkeypatch.setattr(database, '_schema_context', None)
    monkeypatch.setattr(database, '_schema_expires_at', 0)


def test_all_analytical_cases_on_clickhouse_24_3(agent_database):
    for case in CASES:
        result = execute_sql_query(case['sql'])
        assert result.status == 'ok', (case['id'], result)
        assert rows_match(result.rows, case['expected']), (case['id'], result.rows)
    context = get_schema_context()
    assert '2024-11-01' in context.text
    assert 'ride_distance_count' in context.text


def test_server_permissions_and_constraints_are_independent_of_parser(warehouse):
    _, port = warehouse
    analyst = clickhouse_connect.get_client(host='127.0.0.1', port=port,
                                           username='ai_agent', password='integration-agent')
    memory = clickhouse_connect.get_client(host='127.0.0.1', port=port,
                                          username='ai_memory', password='integration-memory')
    try:
        for sql in ["INSERT INTO agent.memory (question, query, embedding) VALUES ('q','q',[])",
                    'SELECT * FROM agent.memory',
                    'SELECT 1 SETTINGS readonly=0',
                    'SELECT 1 SETTINGS max_execution_time=0',
                    'SELECT 1 SETTINGS max_memory_usage=10000000000']:
            with pytest.raises(Exception):
                analyst.query(sql)
        with pytest.raises(Exception):
            memory.query('SELECT count() FROM gold.fact_trips')
        memory.insert('agent.memory', [['q', 'SELECT 1', [0.1], 'v2', 1]],
                      column_names=['question', 'query', 'embedding', 'schema_version', 'validated'])
        assert memory.query('SELECT count() FROM agent.memory').first_row[0] == 1
    finally:
        analyst.close()
        memory.close()


def test_reconciliation_sql_on_native_server(warehouse):
    from jinja2 import Environment

    root, _ = warehouse
    path = Path(ROOT / 'dbt/tests/summary_metrics_reconcile.sql')
    sql = Environment().from_string(path.read_text()).render(
        ref=lambda name: ('gold.' if name == 'fact_trips' else 'marts.') + name)
    assert root.query(sql).result_rows == []


def test_real_memory_roundtrip_similarity_and_deduplication(agent_database, monkeypatch):
    monkeypatch.setenv('AI_MEMORY_PASSWORD', 'integration-memory')
    version = get_schema_context().version
    query = 'SELECT count() FROM gold.fact_trips'
    # Exercise real ClickHouse cosineDistance/grouping, without downloading a model.
    with patch.dict('sys.modules', memory=SimpleNamespace(embed_text=lambda _: [1.0, 0.0])):
        assert save_memory('How many rides?', query, version)
        assert save_memory('How many rides?', query, version)
        memories = retrieve_memory('How many rides?', version)
        assert len(memories) == 1
        assert memories[0]['query'] == query
        assert memories[0]['distance'] == pytest.approx(0)
        assert retrieve_memory('How many rides?', 'different-schema') == []
    with patch.dict('sys.modules', memory=SimpleNamespace(embed_text=lambda _: [0.0, 1.0])):
        assert retrieve_memory('Unrelated question', version) == []


def test_native_result_limit_marks_truncation(agent_database):
    result = execute_sql_query('''SELECT t.ride_id FROM gold.fact_trips t
        CROSS JOIN gold.fact_trips s CROSS JOIN gold.fact_trips v''')
    assert result.status == 'ok'
    assert result.truncated and len(result.rows) == 50
