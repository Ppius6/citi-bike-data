"""Deterministic analytical fixtures and optional live-model result evaluations.

Run `python evaluation.py` for local SQL regressions, or `--live` to measure a
configured model's query results and request metrics against the same fixture.
No production database is read or modified. Live mode incurs provider usage.
"""
import argparse
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import sqlglot
from jinja2 import Environment, StrictUndefined

from contracts import QueryResult
from database import SchemaContext, validate_sql

ROOT = Path(__file__).resolve().parents[2]
TABLES = {
    'gold.fact_trips': '''ride_id VARCHAR, date_key UINTEGER, started_at TIMESTAMP,
        ended_at TIMESTAMP, start_station_key VARCHAR, end_station_key VARCHAR,
        rider_type_key UTINYINT, bike_type_key UTINYINT, weather_code SMALLINT,
        is_daylight UTINYINT, ride_duration_minutes DOUBLE,
        ride_distance_km DOUBLE, temperature_c DOUBLE''',
    'gold.dim_station': '''station_key VARCHAR, station_id VARCHAR, station_name VARCHAR,
        city VARCHAR, is_current UTINYINT''',
    'gold.dim_date': 'date_key UINTEGER, full_date DATE, day_of_week_num UTINYINT, is_weekend UTINYINT',
    'gold.dim_rider_type': 'rider_type_key UTINYINT, rider_type VARCHAR, rider_type_desc VARCHAR',
    'gold.dim_bike_type': 'bike_type_key UTINYINT, bike_type VARCHAR, bike_type_desc VARCHAR',
    'gold.dim_weather_code': 'weather_code SMALLINT, weather_description VARCHAR',
}
DATA = {
    'gold.fact_trips': [
        ('a', 20241005, '2024-10-05 08:00:00', '2024-10-05 08:10:00', 'a-old', 'b', 1, 1, 0, 1, 10, 1, None),
        ('b', 20241005, '2024-10-05 08:00:00', '2024-10-05 08:20:00', 'a-old', 'b', 1, 1, 0, 1, 20, None, 20),
        ('c', 20241006, '2024-10-06 08:00:00', '2024-10-06 09:30:00', 'a-new', 'b', 2, 1, 0, 1, 90, 9, 10),
        ('d', 20241101, '2024-11-01 08:00:00', '2024-11-01 09:00:00', 'a-new', 'b', 2, 1, 0, 1, 60, None, None),
    ],
    'gold.dim_station': [('a-old', 'A', 'Station A old', 'Jersey City', 0),
                         ('a-new', 'A', 'Station A', 'Jersey City', 1),
                         ('b', 'B', 'Station B', 'Hoboken', 1)],
    'gold.dim_rider_type': [(1, 'member', 'Member'), (2, 'casual', 'Casual')],
    'gold.dim_bike_type': [(1, 'classic_bike', 'Classic Bike')],
    'gold.dim_weather_code': [(0, 'Clear sky')],
}
MODEL_NAMES = ['daily_ride_summary', 'mart_city_flow', 'mart_station_flow', 'mart_demand_patterns']
CASES = [
    {'id': 'weighted_duration', 'question': 'What is the average ride duration across all rides?',
     'sql': 'SELECT sum(ride_duration_sum)/nullIf(sum(ride_duration_count),0) FROM marts.daily_ride_summary',
     'expected': [[45.0]]},
    {'id': 'nullable_distance', 'question': 'What is the mean straight-line distance across all rides with known distance?',
     'sql': 'SELECT sum(ride_distance_sum)/nullIf(sum(ride_distance_count),0) FROM marts.daily_ride_summary',
     'expected': [[5.0]]},
    {'id': 'nullable_temperature', 'question': 'What is the average temperature across rides with weather observations?',
     'sql': 'SELECT sum(temperature_sum)/nullIf(sum(temperature_count),0) FROM marts.daily_ride_summary',
     'expected': [[15.0]]},
    {'id': 'city_weighting', 'question': 'What is the average duration of rides from Jersey City to Hoboken across all months?',
     'sql': "SELECT sum(ride_duration_sum)/nullIf(sum(ride_duration_count),0) FROM marts.mart_city_flow WHERE start_city='Jersey City' AND end_city='Hoboken'",
     'expected': [[45.0]]},
    {'id': 'station_scd2', 'question': 'How many rides departed physical station A across all its historical versions?',
     'sql': "SELECT count() FROM gold.fact_trips t JOIN gold.dim_station ds ON t.start_station_key=ds.station_key WHERE ds.station_id='A'",
     'expected': [[4]]},
    {'id': 'quiet_hours', 'question': 'What are typical Sunday 8am departures at station A, and how many dates back that baseline?',
     'sql': "SELECT avg_departures, sample_size FROM marts.mart_demand_patterns WHERE station_id='A' AND day_of_week=7 AND hour=8",
     'expected': [[0.25, 4]]},
    {'id': 'relative_weekend', 'question': 'How many rides were there last weekend?',
     'sql': "SELECT count() FROM gold.fact_trips WHERE toDate(started_at) BETWEEN '2024-10-05' AND '2024-10-06'",
     'expected': [[3]]},
    {'id': 'followup', 'question': 'What about members?',
     'history': [{'role': 'user', 'content': 'How many rides were there on October 5–6, 2024?'},
                 {'role': 'assistant', 'content': 'There were 3 rides on October 5–6, 2024.'}],
     'sql': "SELECT count() FROM gold.fact_trips t JOIN gold.dim_rider_type r ON t.rider_type_key=r.rider_type_key WHERE r.rider_type='member' AND toDate(started_at) BETWEEN '2024-10-05' AND '2024-10-06'",
     'expected': [[2]]},
]


def render_model(name):
    def ref(table):
        return ('marts.' if table.startswith('mart_') or table == 'daily_ride_summary' else 'gold.') + table

    def surrogate(columns):
        return 'concat(' + ", '|', ".join(f'toString({col})' for col in columns) + ')'

    return Environment(undefined=StrictUndefined).from_string(
        (ROOT / 'dbt/models/marts' / f'{name}.sql').read_text()).render(
            config=lambda **_: '', ref=ref, dbt_utils=SimpleNamespace(generate_surrogate_key=surrogate))


def duck_sql(sql):
    return sqlglot.transpile(sql, read='clickhouse', write='duckdb')[0]


def fixture_warehouse():
    import duckdb

    conn = duckdb.connect(':memory:')
    conn.execute('CREATE SCHEMA gold; CREATE SCHEMA marts')
    conn.execute('CREATE MACRO toUInt8(x) AS CAST(x AS UTINYINT)')
    conn.execute('CREATE MACRO toString(x) AS CAST(x AS VARCHAR)')
    conn.execute('CREATE MACRO toHour(x) AS hour(x)')
    conn.execute('CREATE MACRO toDate(x) AS CAST(x AS DATE)')
    conn.execute('CREATE MACRO sumIf(x, condition) AS sum(CASE WHEN condition THEN x ELSE NULL END)')
    conn.execute('CREATE MACRO numbers(n) AS TABLE SELECT range AS number FROM range(n)')
    for table, columns in TABLES.items():
        conn.execute(f'CREATE TABLE {table} ({columns})')
    for table, rows in DATA.items():
        conn.executemany(f'INSERT INTO {table} VALUES ({",".join("?" for _ in rows[0])})', rows)
    conn.execute("""INSERT INTO gold.dim_date
        SELECT CAST(strftime(d, '%Y%m%d') AS UINTEGER), CAST(d AS DATE), isodow(d),
               CAST(isodow(d) >= 6 AS UTINYINT)
        FROM generate_series(DATE '2024-10-01', DATE '2024-11-01', INTERVAL 1 DAY) t(d)""")
    for name in MODEL_NAMES:
        conn.execute(f'CREATE TABLE marts.{name} AS ' + duck_sql(render_model(name)))
    return conn


def fixture_schema(conn):
    rows = conn.execute("""SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns WHERE table_schema IN ('gold', 'marts')
        ORDER BY table_schema, table_name, ordinal_position""").fetchall()
    return SchemaContext('\n'.join(f'{db}.{table}.{col} {kind}' for db, table, col, kind in rows)
                         + '\nTrip start-date coverage: 2024-10-05 through 2024-11-01.', 'fixture-v2')


def fixture_execute(conn, query, **_):
    try:
        normalized, sources = validate_sql(query)
        cursor = conn.execute(duck_sql(normalized))
        rows = cursor.fetchmany(51)
        return QueryResult(status='ok' if rows else 'empty', rows=[list(row) for row in rows[:50]],
                           columns=[col[0] for col in cursor.description],
                           source_tables=sources, truncated=len(rows) > 50)
    except Exception as error:
        return QueryResult(status='error', error=str(error))


def rows_match(actual, expected):
    if len(actual) != len(expected):
        return False
    for row, target in zip(actual, expected):
        if len(row) != len(target):
            return False
        for value, wanted in zip(row, target):
            if isinstance(wanted, (int, float)):
                if not isinstance(value, (int, float)) or abs(value - wanted) > 1e-6:
                    return False
            elif value != wanted:
                return False
    return True


def evaluate(live=False):
    conn = fixture_warehouse()
    reports = []
    try:
        if live:
            import agent

            for case in CASES:
                results = []

                def execute(query, **kwargs):
                    result = fixture_execute(conn, query, **kwargs)
                    results.append(result)
                    return result

                with (patch('agent.get_schema_context', return_value=fixture_schema(conn)),
                      patch('agent.retrieve_memory', return_value=[]),
                      patch('agent.create_candidate', return_value=None),
                      patch('agent.execute_sql_query', side_effect=execute),
                      patch('agent.date_context', return_value='Current New York date: 2024-10-07. Last completed weekend: 2024-10-05 through 2024-10-06.')):
                    answer = agent.run_bike_agent(case['question'], case.get('history'))
                reports.append({'case': case['id'], 'query_result_correct': bool(results) and
                                results[-1].status == 'ok' and rows_match(results[-1].rows, case['expected']),
                                'answer': answer['answer'], 'metrics': answer['metrics']})
        else:
            for case in CASES:
                result = fixture_execute(conn, case['sql'])
                reports.append({'case': case['id'], 'query_result_correct': result.status == 'ok' and
                                rows_match(result.rows, case['expected']), 'error': result.error})
        return reports
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true', help='Call the configured LLM (incurs API usage).')
    args = parser.parse_args()
    if args.live:
        from dotenv import load_dotenv

        load_dotenv(ROOT / '.env')
    report = evaluate(args.live)
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if all(row['query_result_correct'] for row in report) else 1)
