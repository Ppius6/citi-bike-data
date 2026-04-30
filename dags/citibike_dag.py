"""Airflow DAG for Citi Bike ETL and analytics pipeline."""

import logging
import sys
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Add dags directory to Python path for custom modules
sys.path.append("/opt/airflow/dags")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "citibike_pipeline_production",
    default_args=default_args,
    description="Production Citi Bike data pipeline",
    schedule_interval="0 6 * * *",  # Run daily at 6 AM
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "data-engineering", "production"],
)


def setup_database_schema(**context):
    """Create the database schema and tables if they don't exist."""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="citibike_postgres")
        schema_sql = """
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.trips (
            ride_id VARCHAR(255) PRIMARY KEY,
            rideable_type VARCHAR(50),
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            start_station_name VARCHAR(255),
            start_station_id VARCHAR(50),
            end_station_name VARCHAR(255),
            end_station_id VARCHAR(50),
            start_lat DECIMAL(10,8),
            start_lng DECIMAL(11,8),
            end_lat DECIMAL(10,8),
            end_lng DECIMAL(11,8),
            member_casual VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS pipeline_file_log (
            file_name VARCHAR(255) PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            records_processed INTEGER,
            file_size_mb DECIMAL(10,2),
            processing_duration_seconds INTEGER,
            status VARCHAR(50) DEFAULT 'completed'
        );
        """
        postgres_hook.run(schema_sql)
        logger.info("Database schema and tables created successfully")
        return "Database setup completed"
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise


def detect_new_files(**context):
    """Detect new files that need processing."""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="citibike_postgres")
        from config import config

        response = requests.get(config["base_url"], timeout=30)
        response.raise_for_status()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(response.text, "xml")
        available_files = [
            key.text
            for key in soup.find_all("Key")
            if key.text.startswith("JC") and key.text.endswith(".zip")
        ]
        if not available_files:
            logger.warning("No files found in S3 bucket")
            return "no_new_files"
        try:
            query = "SELECT file_name FROM pipeline_file_log WHERE status = 'completed'"
            result = postgres_hook.get_records(query)
            processed_files = {row[0] for row in result}
        except Exception:
            processed_files = set()
        start_from = config.get("start_from", "JC-202102-citibike-tripdata.csv.zip")
        all_files_sorted = sorted(available_files)
        try:
            start_index = all_files_sorted.index(start_from)
            logger.info(f"Starting from file: {start_from} (index {start_index})")
        except ValueError:
            logger.warning(
                f"Start file {start_from} not found, starting from beginning"
            )
            start_index = 0
        unprocessed_files = [
            f for f in all_files_sorted[start_index:] if f not in processed_files
        ]
        if not unprocessed_files:
            logger.info("No new files to process")
            return "no_new_files"
        batch_size = config.get("batch_size", 3)
        new_files = unprocessed_files[:batch_size]
        context["task_instance"].xcom_push(key="files_to_process", value=new_files)
        logger.info(f"Total unprocessed files: {len(unprocessed_files)}")
        logger.info(f"Processing {len(new_files)} files in this run: {new_files}")
        return "process_new_files"
    except Exception as e:
        logger.error(f"Error in new file detection: {e}")
        raise


def run_pipeline(**context):
    """Run the CitiBike pipeline for new files."""
    try:
        files_to_process = context["task_instance"].xcom_pull(
            task_ids="detect_new_files", key="files_to_process"
        )
        if not files_to_process:
            logger.info("No files to process")
            return "No new files found"
        from citibike_pipeline import CitiBikePipeline

        from config import config

        pipeline = CitiBikePipeline(config.copy())
        postgres_hook = PostgresHook(postgres_conn_id="citibike_postgres")
        total_records = 0
        for file_name in files_to_process:
            try:
                logger.info(f"Processing file: {file_name}")
                csv_path = pipeline.download_and_extract(file_name)
                if csv_path:
                    records = pipeline.load_csv_to_db(csv_path)
                    total_records += records
                    log_sql = """
                    INSERT INTO pipeline_file_log (file_name, records_processed, status)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (file_name) DO UPDATE SET
                        processed_at = CURRENT_TIMESTAMP,
                        records_processed = EXCLUDED.records_processed,
                        status = EXCLUDED.status;
                    """
                    postgres_hook.run(
                        log_sql, parameters=(file_name, records, "completed")
                    )
                    logger.info(
                        f"Successfully processed {file_name}: {records} records"
                    )
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {e}")
                log_sql = """
                INSERT INTO pipeline_file_log (file_name, records_processed, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (file_name) DO UPDATE SET
                    processed_at = CURRENT_TIMESTAMP,
                    status = EXCLUDED.status;
                """
                postgres_hook.run(log_sql, parameters=(file_name, 0, "failed"))
        logger.info(f"Pipeline completed. Total records processed: {total_records}")
        return f"Processed {len(files_to_process)} files, {total_records} total records"
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


def run_data_quality_checks(**context):
    """Run data quality checks."""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="citibike_postgres")
        checks = {
            "total_records": "SELECT COUNT(*) FROM raw.trips",
            "null_ride_ids": "SELECT COUNT(*) FROM raw.trips WHERE ride_id IS NULL",
            "invalid_durations": "SELECT COUNT(*) FROM raw.trips WHERE started_at >= ended_at",
            "recent_data": "SELECT COUNT(*) FROM raw.trips WHERE DATE(created_at) >= CURRENT_DATE - INTERVAL '7 days'",
        }
        results = {}
        for check_name, query in checks.items():
            try:
                result = postgres_hook.get_first(query)[0]
                results[check_name] = result
                logger.info(f"Quality check {check_name}: {result}")
            except Exception as e:
                logger.error(f"Quality check {check_name} failed: {e}")
                results[check_name] = -1
        issues = []
        if results.get("null_ride_ids", 0) > 0:
            issues.append(
                f"Found {results['null_ride_ids']} records with null ride_ids"
            )
        if results.get("invalid_durations", 0) > 0:
            issues.append(
                f"Found {results['invalid_durations']} records with invalid durations"
            )
        if issues:
            logger.warning(f"Quality issues found: {issues}")
        else:
            logger.info("All quality checks passed")
        context["task_instance"].xcom_push(key="quality_results", value=results)
        context["task_instance"].xcom_push(key="quality_issues", value=issues)
        return f"Quality checks completed. {len(issues)} issues found."
    except Exception as e:
        logger.error(f"Quality checks failed: {e}")
        raise


def run_dbt_transformations(**context):
    """Run dbt transformations - Python implementation of your dbt models."""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="citibike_postgres")
        logger.info("Starting dbt transformations...")
        postgres_hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")
        stg_trips_sql = """
        CREATE OR REPLACE VIEW analytics.stg_trips AS
        SELECT 
            ride_id,
            rideable_type,
            started_at,
            ended_at,
            start_station_name,
            start_station_id,
            end_station_name,
            end_station_id,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            member_casual,
            EXTRACT(EPOCH FROM (ended_at - started_at))/60 AS trip_duration_minutes,
            EXTRACT(HOUR FROM started_at) AS start_hour,
            DATE(started_at) AS trip_date,
            EXTRACT(DOW FROM started_at) AS day_of_week,
            CASE 
                WHEN EXTRACT(HOUR FROM started_at) BETWEEN 7 AND 9 
                    OR EXTRACT(HOUR FROM started_at) BETWEEN 17 AND 19 
                THEN TRUE 
                ELSE FALSE 
            END AS is_rush_hour
        FROM raw.trips
        WHERE started_at IS NOT NULL 
            AND ended_at IS NOT NULL
            AND started_at < ended_at
            AND EXTRACT(EPOCH FROM (ended_at - started_at))/60 BETWEEN 1 AND 1440;
        """
        postgres_hook.run(stg_trips_sql)
        dim_stations_sql = """
        DROP TABLE IF EXISTS analytics.dim_stations;
        CREATE TABLE analytics.dim_stations AS
        WITH station_data AS (
            SELECT 
                start_station_id AS station_id,
                start_station_name AS station_name,
                AVG(start_lat) AS latitude,
                AVG(start_lng) AS longitude
            FROM analytics.stg_trips
            WHERE start_station_id IS NOT NULL
            GROUP BY start_station_id, start_station_name
            UNION ALL
            SELECT 
                end_station_id AS station_id,
                end_station_name AS station_name,
                AVG(end_lat) AS latitude,
                AVG(end_lng) AS longitude
            FROM analytics.stg_trips
            WHERE end_station_id IS NOT NULL
            GROUP BY end_station_id, end_station_name
        ),
        unique_stations AS (
            SELECT 
                station_id,
                station_name,
                AVG(latitude) AS latitude,
                AVG(longitude) AS longitude
            FROM station_data
            GROUP BY station_id, station_name
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY station_id) AS station_key,
            station_id,
            station_name,
            latitude,
            longitude,
            CURRENT_TIMESTAMP AS created_at
        FROM unique_stations
        WHERE station_id IS NOT NULL;
        """
        postgres_hook.run(dim_stations_sql)
        fact_trips_sql = """
        DROP TABLE IF EXISTS analytics.fact_trips;
        CREATE TABLE analytics.fact_trips AS
        SELECT 
            t.ride_id,
            t.rideable_type,
            t.member_casual,
            t.trip_duration_minutes,
            t.start_hour,
            t.trip_date,
            t.day_of_week,
            t.is_rush_hour,
            ss.station_name as start_station_name,
            ss.latitude as start_lat,
            ss.longitude as start_lng,
            es.station_name as end_station_name,
            es.latitude as end_lat,
            es.longitude as end_lng,
            t.started_at,
            t.ended_at
        FROM analytics.stg_trips t
        LEFT JOIN analytics.dim_stations ss 
            ON t.start_station_id = ss.station_id
        LEFT JOIN analytics.dim_stations es 
            ON t.end_station_id = es.station_id;
        """
        postgres_hook.run(fact_trips_sql)
        monthly_summary_sql = """
        DROP TABLE IF EXISTS analytics.monthly_summary;
        CREATE TABLE analytics.monthly_summary AS
        SELECT 
            EXTRACT(YEAR FROM trip_date)::INTEGER as year,
            EXTRACT(MONTH FROM trip_date)::INTEGER as month,
            COUNT(*) as total_trips,
            ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) as avg_duration_minutes,
            COUNT(CASE WHEN member_casual = 'member' THEN 1 END) as member_trips,
            COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) as casual_trips,
            ROUND((COUNT(CASE WHEN member_casual = 'member' THEN 1 END) * 100.0 / COUNT(*))::NUMERIC, 2) as member_percentage,
            COUNT(CASE WHEN is_rush_hour = TRUE THEN 1 END) as rush_hour_trips
        FROM analytics.fact_trips
        GROUP BY EXTRACT(YEAR FROM trip_date), EXTRACT(MONTH FROM trip_date)
        ORDER BY year, month;
        """
        postgres_hook.run(monthly_summary_sql)
        station_popularity_sql = """
        DROP TABLE IF EXISTS analytics.station_popularity;
        CREATE TABLE analytics.station_popularity AS
        SELECT 
            start_station_name as station_name,
            COUNT(*) as trip_count,
            COUNT(CASE WHEN member_casual = 'member' THEN 1 END) as member_trips,
            COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) as casual_trips,
            ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) as avg_duration_minutes,
            COUNT(CASE WHEN is_rush_hour = TRUE THEN 1 END) as rush_hour_trips
        FROM analytics.fact_trips
        WHERE start_station_name IS NOT NULL
        GROUP BY start_station_name
        HAVING COUNT(*) >= 10
        ORDER BY trip_count DESC;
        """
        postgres_hook.run(station_popularity_sql)
        summary_queries = {
            "stg_trips": "SELECT COUNT(*) FROM analytics.stg_trips",
            "dim_stations": "SELECT COUNT(*) FROM analytics.dim_stations",
            "fact_trips": "SELECT COUNT(*) FROM analytics.fact_trips",
            "monthly_summary": "SELECT COUNT(*) FROM analytics.monthly_summary",
            "station_popularity": "SELECT COUNT(*) FROM analytics.station_popularity",
        }
        results = {}
        for table_name, query in summary_queries.items():
            try:
                count = postgres_hook.get_first(query)[0]
                results[table_name] = count
                logger.info(f"{table_name}: {count:,} records")
            except Exception as e:
                logger.error(f"Error getting count for {table_name}: {e}")
                results[table_name] = 0
        try:
            date_range_query = """
            SELECT 
                MIN(trip_date) as earliest_date,
                MAX(trip_date) as latest_date,
                COUNT(DISTINCT trip_date) as unique_days
            FROM analytics.fact_trips
            """
            date_result = postgres_hook.get_first(date_range_query)
            logger.info(
                f"Date range: {date_result[0]} to {date_result[1]} ({date_result[2]} unique days)"
            )
        except Exception as e:
            logger.error(f"Error getting date range: {e}")
        logger.info("All dbt transformations completed successfully!")
        total_trips = results.get("fact_trips", 0)
        return f"dbt transformations completed - {total_trips:,} trips processed"
    except Exception as e:
        logger.error(f"dbt transformations failed: {e}")
        raise


def send_notification(**context):
    """Send completion notification."""
    try:
        quality_results = context["task_instance"].xcom_pull(
            task_ids="data_quality_check", key="quality_results"
        )
        pipeline_result = context["task_instance"].xcom_pull(task_ids="run_pipeline")
        message = f"""
        CitiBike Pipeline Completed Successfully

        Processing Summary:
        • Pipeline result: {pipeline_result}
        • Total records in DB: {quality_results.get('total_records', 'N/A') if quality_results else 'N/A'}
        • Recent data (7 days): {quality_results.get('recent_data', 'N/A') if quality_results else 'N/A'}
        • Run date: {context['ds']}
        • Next scheduled run: {context.get('next_ds', 'N/A')}
        """
        logger.info(message)
        return "Notification sent successfully"
    except Exception as e:
        logger.error(f"Notification failed: {e}")
        return "Notification failed"


# Define tasks
setup_db = PythonOperator(
    task_id="setup_database_schema",
    python_callable=setup_database_schema,
    dag=dag,
)

detect_files = BranchPythonOperator(
    task_id="detect_new_files", python_callable=detect_new_files, dag=dag
)

no_new_files = DummyOperator(task_id="no_new_files", dag=dag)

run_pipeline_task = PythonOperator(
    task_id="process_new_files", python_callable=run_pipeline, dag=dag
)

data_quality_check = PythonOperator(
    task_id="data_quality_check", python_callable=run_data_quality_checks, dag=dag
)

dbt_run = PythonOperator(
    task_id="dbt_run",
    python_callable=run_dbt_transformations,
    dag=dag,
)

notification = PythonOperator(
    task_id="send_notification",
    python_callable=send_notification,
    dag=dag,
)

# Task dependencies
setup_db >> detect_files >> [no_new_files, run_pipeline_task]
run_pipeline_task >> data_quality_check >> dbt_run >> notification
