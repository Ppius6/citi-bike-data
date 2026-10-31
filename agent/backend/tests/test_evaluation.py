import pytest

from evaluation import CASES, evaluate, fixture_execute, fixture_warehouse, rows_match


def test_analytical_evaluation_results():
    report = evaluate()
    assert all(case['query_result_correct'] for case in report), report


@pytest.fixture
def warehouse():
    conn = fixture_warehouse()
    yield conn
    conn.close()


def test_fixture_detects_average_of_averages_and_wrong_null_denominator(warehouse):
    bad_duration = fixture_execute(warehouse, 'SELECT avg(avg_ride_duration_minutes) FROM marts.daily_ride_summary')
    assert not rows_match(bad_duration.rows, CASES[0]['expected'])
    bad_distance = fixture_execute(warehouse, 'SELECT sum(avg_ride_distance_km * total_rides)/sum(total_rides) FROM marts.daily_ride_summary')
    assert not rows_match(bad_distance.rows, CASES[1]['expected'])


def test_missing_metric_fails_instead_of_fabricating_data(warehouse):
    result = fixture_execute(warehouse, 'SELECT avg(humidity) FROM gold.fact_trips')
    assert result.status == 'error'


def test_natural_key_join_would_duplicate_rides(warehouse):
    bad = fixture_execute(warehouse, """
        SELECT count() FROM gold.fact_trips t
        JOIN gold.dim_station historical ON t.start_station_key=historical.station_key
        JOIN gold.dim_station versions ON historical.station_id=versions.station_id
        WHERE versions.station_id='A'
    """)
    assert bad.rows == [[8]]
    assert not rows_match(bad.rows, CASES[4]['expected'])
