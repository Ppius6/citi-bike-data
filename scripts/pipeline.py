import logging
import logging.config
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from scripts.config.config import Config
from scripts.ingestion.ingest import Downloader
from scripts.loading.loader import BronzeLoader
from scripts.storage.minio_client import MinIOClient


def setup_logging() -> None:
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                },
                "file": {
                    "class": "logging.FileHandler",
                    "filename": "pipeline.log",
                    "formatter": "standard",
                },
            },
            "root": {
                "handlers": ["console", "file"],
                "level": "INFO",
            },
        }
    )


logger = logging.getLogger(__name__)


def process_file(
    file_name: str, downloader: Downloader, minio: MinIOClient
) -> Optional[str]:
    """
    Full processing for a single file:
      1. Download zip to temp
      2. Convert to Parquet buffer
      3. Upload to MinIO

    Returns the MinIO key if successful, None otherwise.
    """
    zip_path = downloader.download_to_temp(file_name)
    if zip_path is None:
        return None

    result = downloader.to_parquet_buffer(zip_path)
    if result is None:
        return None

    buffer, key = result
    minio.upload_parquet(buffer, key)
    return key


def run_ingest(config: Config) -> bool:
    """
    Phase 1 — fetch, download, convert, and upload Parquet files to MinIO.
    Returns True if completed without fatal errors.
    """
    downloader = Downloader(config.source, config.pipeline)
    minio = MinIOClient(config.minio)

    logger.info("Ingest started.")

    try:
        file_list = downloader.get_file_list()
    except Exception as e:
        logger.error(f"Failed to fetch file list: {e}")
        return False

    if not file_list:
        logger.info("No files found at source.")
        return True

    new_files = [
        f
        for f in file_list
        if not minio.object_exists(f"bronze/{f.replace('.zip', '.parquet')}")
    ]

    if not new_files:
        logger.info("All files already in MinIO.")
        return True

    logger.info(f"{len(new_files)} new file(s) to ingest.")

    uploaded = []
    failed = []

    with ThreadPoolExecutor(max_workers=config.pipeline.max_workers) as executor:
        futures = {
            executor.submit(process_file, f, downloader, minio): f for f in new_files
        }

        for future in as_completed(futures):
            file_name = futures[future]
            try:
                key = future.result()
                if key:
                    uploaded.append(key)
                    logger.info(f"Ingested: {file_name}")
                else:
                    failed.append(file_name)
                    logger.warning(f"Ingest returned no result: {file_name}")
            except Exception as e:
                failed.append(file_name)
                logger.error(f"Unhandled error ingesting {file_name}: {e}")

    logger.info(f"Ingest complete. Uploaded: {len(uploaded)} | Failed: {len(failed)}")

    if failed:
        logger.warning(f"Failed files: {failed}")

    return len(failed) == 0


def run_bronze_load(config: Config) -> bool:
    """
    Load Parquet files from MinIO into Postgres bronze.trips.
    Returns True if completed without fatal errors.
    """
    minio = MinIOClient(config.minio)
    loader = BronzeLoader(config.postgres, minio)

    logger.info("Bronze load started.")
    success = loader.run()

    if success:
        logger.info("Bronze load complete.")
    else:
        logger.error("Bronze load finished with failures.")

    return success


def run(config: Optional[Config] = None) -> bool:
    """
    Execute the full pipeline:
      1. Ingest — download source files, convert to Parquet, land in MinIO
      2. Bronze load — read Parquet from MinIO, load into Postgres bronze.trips

    Returns True if both phases completed without fatal errors.
    """

    if config is None:
        config = Config()

    setup_logging()
    logger.info("Pipeline started.")

    if not run_ingest(config):
        logger.error("Ingest phase failed. Aborting.")
        return False

    if not run_bronze_load(config):
        logger.error("Bronze load phase failed.")
        return False

    logger.info("Pipeline completed successfully.")
    return True


if __name__ == "__main__":
    success = run()
    exit(0 if success else 1)
