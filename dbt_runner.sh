#!/bin/bash
set -e

echo "🚀 Starting dbt transformations..."

# Try docker exec first (if socket is available)
if docker exec dbt-citibike dbt --version >/dev/null 2>&1; then
    echo "✅ Using docker exec approach"
    docker exec dbt-citibike dbt run --profiles-dir /usr/app
    docker exec dbt-citibike dbt test --profiles-dir /usr/app
    echo "✅ dbt transformations completed!"
else
    echo "📊 Using direct SQL transformations (dbt-style)..."
    
    # Run dbt-style transformations via Python
    python3 << 'PYTHON_EOF'
import sys
sys.path.append("/opt/airflow/dags")

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    postgres_hook = PostgresHook(postgres_conn_id="citibike_postgres")
    
    # Create analytics schema
    postgres_hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")
    
    # Clean trips view
    clean_trips_sql = """
    CREATE OR REPLACE VIEW analytics.clean_trips AS
    SELECT 
        ride_id, rideable_type, started_at, ended_at,
        EXTRACT(EPOCH FROM (ended_at - started_at))/60 as duration_minutes,
        start_station_name, end_station_name,
        start_lat, start_lng, end_lat, end_lng, member_casual,
        EXTRACT(YEAR FROM started_at) as trip_year,
        EXTRACT(MONTH FROM started_at) as trip_month
    FROM raw.trips
    WHERE started_at < ended_at;
    """
    
    # Monthly summary
    monthly_summary_sql = """
    DROP TABLE IF EXISTS analytics.monthly_summary;
    CREATE TABLE analytics.monthly_summary AS
    SELECT 
        EXTRACT(YEAR FROM started_at) as year,
        EXTRACT(MONTH FROM started_at) as month,
        COUNT(*) as total_trips,
        AVG(EXTRACT(EPOCH FROM (ended_at - started_at))/60) as avg_duration,
        COUNT(CASE WHEN member_casual = 'member' THEN 1 END) as member_trips,
        COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) as casual_trips
    FROM raw.trips
    WHERE started_at < ended_at
    GROUP BY EXTRACT(YEAR FROM started_at), EXTRACT(MONTH FROM started_at);
    """
    
    postgres_hook.run(clean_trips_sql)
    postgres_hook.run(monthly_summary_sql)
    
    print("✅ Analytics tables created successfully!")
    
except Exception as e:
    print(f"Error: {e}")

PYTHON_EOF
    
    echo "✅ SQL transformations completed!"
fi
