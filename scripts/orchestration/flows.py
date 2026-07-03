import logging
import subprocess
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.blocks.notifications import SlackWebhook

from scripts.config.config import Config
from scripts.pipeline import run_ingest, run_bronze_load
from scripts.quality.validator import run_quality_checks

from datetime import timedelta

# Absolute path to the dbt project
DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "dbt"


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
    name="quality_check_bronze",
    description="Run Soda Core quality checks on bronze.trips",
    retries=1,
    retry_delay_seconds=30,
)
def quality_check_task(config: Config) -> bool:
    logger = get_run_logger()
    logger.info("Running quality checks on bronze.trips.")

    checks_path = (
        Path(__file__).resolve().parent.parent.parent
        / "scripts"
        / "quality"
        / "checks.yml"
    )

    connection_config = {
        "host": config.postgres.host,
        "port": config.postgres.port,
        "user": config.postgres.user,
        "password": config.postgres.password,
        "database": config.postgres.name,
    }

    success = run_quality_checks("bronze", checks_path, connection_config)

    if not success:
        raise RuntimeError("Quality checks failed — pipeline halted.")
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
    success = run_dbt(
        "run --select int_trips_cleaned silver_trips silver_trips_rejected --target dev"
    )
    if not success:
        raise RuntimeError("dbt silver run failed.")
    return success


@task(
    name="dbt_elementary",
    description="Initialise Elementary models in silver schema",
    retries=1,
    retry_delay_seconds=30,
)
def dbt_elementary_task() -> bool:
    logger = get_run_logger()
    logger.info("Running Elementary setup.")
    success = run_dbt("run --select elementary --target dev")
    if not success:
        raise RuntimeError("Elementary setup failed.")
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


@task(
    name="dbt_docs_generate",
    description="Generate dbt docs (model docs, column descriptions, tests, lineage) as the project catalog",
    retries=1,
    retry_delay_seconds=30,
)
def dbt_docs_generate_task() -> bool:
    logger = get_run_logger()
    logger.info("Generating dbt docs artifacts.")
    success = run_dbt("docs generate --target dev")
    if not success:
        raise RuntimeError("dbt docs generate failed.")
    return success


def alert_on_failure(flow, flow_run, state):
    try:
        slack = SlackWebhook.load("citibike-alerts")
        slack.notify(
            f"Pipeline failed: {flow_run.name}\n"
            f"State: {state.message}\n"
            f"View: http://localhost:4200/runs/flow-run/{flow_run.id}"
        )
    except Exception as e:
        logging.getLogger(__name__).warning(f"Slack alert failed (non-fatal): {e}")


@flow(
    name="citibike_pipeline",
    description="Citi Bike data pipeline: Ingest from S3 to MinIO, load to Postgres, transform with dbt, test all layers",
    log_prints=True,
    on_failure=[alert_on_failure],
)
def citibike_pipeline():
    logger = get_run_logger()
    config = Config()

    logger.info("Pipeline flow started.")

    # Ingest to MinIO
    ingest_task(config)

    # Bronze and silver in Postgres
    bronze_load_task(config)
    quality_check_task(config)
    dbt_bronze_task()
    dbt_silver_task()
    dbt_elementary_task()

    # Snapshot and Gold in ClickHouse
    dbt_snapshot_task()
    dbt_gold_task()

    # Tests across all layers
    dbt_test_task(target="dev", models="bronze_trips silver_trips")
    dbt_test_task(target="clickhouse", models="gold.*")

    # Docs as the project catalog
    dbt_docs_generate_task()

    logger.info("Pipeline flow completed successfully.")


if __name__ == "__main__":
    citibike_pipeline()
