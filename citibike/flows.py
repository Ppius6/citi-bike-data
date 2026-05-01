import logging
import subprocess
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

from citibike.config import Config
from citibike.pipeline import run_ingest, run_bronze_load

# Absolute path to the dbt project
DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt" / "citibike_dbt"


def run_dbt(command: str) -> bool:
    """
    Run a dbt command as a subprocess.

    Returns:
        bool: True if successful, False otherwise.
    """
    logger = get_run_logger()
    full_command = f"dbt {command} --profiles-dir ."

    logger.info(f"Running: {full_command}")

    result = subprocess.run(
        full_command,
        shell=True,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)

    if result.returncode != 0:
        logger.error(f"dbt command failed: {full_command}")
        return False

    return True


@task(
    name="ingest",
    description="Download source files from Citi Bike S3, convert to Parquet, land in MinIO",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24),
)
def ingest_task(config: Config) -> bool:
    logger = get_run_logger()
    logger.info("Starting ingest phase.")
    success = run_ingest(config)
    if not success:
        raise RuntimeError("Ingest phase failed.")
    return success


@task(
    name="bronze_load",
    description="Load Parquet files from MinIO into Postgres bronze.trips",
    retries=2,
    retry_delay_seconds=30,
)
def bronze_load_task(config: Config) -> bool:
    logger = get_run_logger()
    logger.info("Starting bronze load phase.")
    success = run_bronze_load(config)
    if not success:
        raise RuntimeError("Bronze load phase failed.")
    return success


@task(
    name="dbt_bronze",
    description="Run dbt bronze models against Postgres",
    retries=2,
    retry_delay_seconds=30,
)
def dbt_bronze_task() -> bool:
    logger = get_run_logger()
    logger.info("Running dbt bronze models.")
    success = run_dbt("run --select bronze_trips --target dev")
    if not success:
        raise RuntimeError("dbt bronze run failed.")
    return success


@task(
    name="dbt_silver",
    description="Run dbt silver models against Postgres",
    retries=2,
    retry_delay_seconds=30,
)
def dbt_silver_task() -> bool:
    logger = get_run_logger()
    logger.info("Running dbt silver models.")
    success = run_dbt("run --select silver_trips --target dev")
    if not success:
        raise RuntimeError("dbt silver run failed.")
    return success


@task(
    name="dbt_snapshot",
    description="Run dbt snapshot for SCD Type 2 station history",
    retries=2,
    retry_delay_seconds=30,
)
def dbt_snapshot_task() -> bool:
    logger = get_run_logger()
    logger.info("Running dbt snapshot.")
    success = run_dbt("snapshot --target dev")
    if not success:
        raise RuntimeError("dbt snapshot failed.")
    return success


@task(
    name="dbt_gold",
    description="Run dbt gold models against ClickHouse",
    retries=2,
    retry_delay_seconds=30,
)
def dbt_gold_task() -> bool:
    logger = get_run_logger()
    logger.info("Running dbt gold models.")
    success = run_dbt("run --select gold.* --target clickhouse")
    if not success:
        raise RuntimeError("dbt gold run failed.")
    return success


@task(
    name="dbt_test",
    description="Run dbt tests across all layers",
    retries=1,
    retry_delay_seconds=30,
)
def dbt_test_task(target: str = "dev", models: str = "") -> bool:
    logger = get_run_logger()
    logger.info(f"Running dbt tests against {target} target.")
    select = f"--select {models}" if models else ""
    success = run_dbt(f"test {select} --target {target}")
    if not success:
        raise RuntimeError(f"dbt tests failed on {target} target.")
    return success


@flow(
    name="citibike_pipeline",
    description="Full Citi Bike data pipeline — ingest to gold layer",
    log_prints=True,
)
def citibike_pipeline():
    logger = get_run_logger()
    config = Config()

    logger.info("Pipeline flow started.")

    # Ingest to MinIO
    ingest_task(config)

    # Bronze and silver in Postgres
    bronze_load_task(config)
    dbt_bronze_task()
    dbt_silver_task()

    # Snapshot and Gold in ClickHouse
    dbt_snapshot_task()
    dbt_gold_task()

    # Tests across all layers
    dbt_test_task(target="dev", models="bronze_trips silver_trips")
    dbt_test_task(target="clickhouse", models="gold.*")

    logger.info("Pipeline flow completed successfully.")


if __name__ == "__main__":
    citibike_pipeline()
