import logging
import logging.config
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from citibike.config import Config
from citibike.ingest import Downloader
from citibike.storage import MinIOClient


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


def run(config: Optional[Config] = None) -> bool:
    """
    Execute the full Phase 1 pipeline:
      1. Fetch the file list from the Citi Bike S3 bucket
      2. Filter out files already in MinIO
      3. Download, convert, and upload new files in parallel

    Returns True if the run completed without fatal errors.
    """
    if config is None:
        config = Config()

    setup_logging()

    downloader = Downloader(config.source, config.pipeline)
    minio = MinIOClient(config.minio)

    logger.info("Pipeline started.")

    # Step 1 — get the full file list from source
    try:
        file_list = downloader.get_file_list()
    except Exception as e:
        logger.error(f"Failed to fetch file list: {e}")
        return False

    if not file_list:
        logger.info("No files found. Exiting.")
        return True

    # Step 2 — filter out files already uploaded to MinIO
    new_files = [
        f
        for f in file_list
        if not minio.object_exists(f"bronze/{f.replace('.zip', '.parquet')}")
    ]

    if not new_files:
        logger.info("All files already in MinIO. Nothing to do.")
        return True

    logger.info(f"{len(new_files)} new file(s) to process.")

    # Step 3 — download, convert, and upload in parallel
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
                    logger.info(f"Successfully processed: {file_name}")
                else:
                    failed.append(file_name)
                    logger.warning(f"Processing returned no result: {file_name}")
            except Exception as e:
                failed.append(file_name)
                logger.error(f"Unhandled error processing {file_name}: {e}")

    # Step 4 — summary
    logger.info(f"Pipeline complete. Uploaded: {len(uploaded)} | Failed: {len(failed)}")

    if failed:
        logger.warning(f"Failed files: {failed}")

    return len(failed) == 0


if __name__ == "__main__":
    success = run()
    exit(0 if success else 1)
