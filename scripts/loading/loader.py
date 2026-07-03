import logging
from io import BytesIO, StringIO

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from scripts.config.config import PostgresConfig
from scripts.storage.minio_client import MinIOClient

import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class BronzeLoader:
    def __init__(self, postgres: PostgresConfig, minio: MinIOClient):
        self.postgres = postgres
        self.minio = minio
        self.engine = create_engine(
            self.postgres.url,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
        )

    def _table_ref(self) -> str:
        return f"{self.postgres.bronze_schema}.trips"

    def get_loaded_sources(self) -> set[str]:
        """
        Return the set of _source_file values already present in bronze.trips.
        Used to skip Parquet files that have already been loaded.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT DISTINCT _source_file FROM {self._table_ref()}")
                )
                return {row[0] for row in result}
        except SQLAlchemyError:
            # Table does not exist yet on first run
            return set()

    def load_parquet(self, key: str) -> bool:
        """
        Download a single Parquet file from MinIO by key,
        read into a DataFrame, and bulk-load it into bronze.trips via COPY.

        Returns:
            bool: True if successful, False if otherwise.
        """
        try:
            response = self.minio.client.get_object(
                Bucket=self.minio.config.bucket,
                Key=key,
            )
            buffer = BytesIO(response["Body"].read())
            df = pd.read_parquet(buffer, engine="pyarrow")

            if df.empty:
                logger.warning(f"Empty Parquet file, skipping: {key}")
                return True

            row_count = len(df)

            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)

            columns = ", ".join(df.columns)
            copy_sql = (
                f"COPY {self.postgres.bronze_schema}.trips ({columns}) "
                "FROM STDIN WITH (FORMAT csv)"
            )

            raw_conn = self.engine.raw_connection()
            try:
                with raw_conn.cursor() as cursor:
                    cursor.copy_expert(copy_sql, csv_buffer)
                raw_conn.commit()
            finally:
                raw_conn.close()

            logger.info(f"Successfully loaded {row_count} rows from {key} via COPY")
            return True

        except Exception as e:
            logger.error(f"Failed to load {key}: {e}")
            return False

    def run(self) -> bool:
        start_time = time.time()

        all_keys = self.minio.list_objects(prefix="bronze/")

        if not all_keys:
            logger.info("No Parquet files found in MinIO bronze/.")
            self._log_metrics(0, 0, start_time)
            return True

        loaded_sources = self.get_loaded_sources()
        logger.info(
            f"Found {len(loaded_sources)} already-loaded source files in bronze.trips."
        )

        new_keys = [
            key
            for key in all_keys
            if key.split("/")[-1].replace(".parquet", "") not in loaded_sources
        ]

        if not new_keys:
            logger.info("All Parquet files already loaded into bronze.trips.")
            self._log_metrics(0, 0, start_time)
            return True

        logger.info(f"{len(new_keys)} new Parquet file(s) to load.")

        failed = []
        for key in new_keys:
            success = self.load_parquet(key)
            if not success:
                failed.append(key)

        loaded = len(new_keys) - len(failed)
        logger.info(f"Bronze load complete. Loaded: {loaded} | Failed: {len(failed)}")

        if failed:
            logger.warning(f"Failed keys: {failed}")

        self._log_metrics(loaded, len(failed), start_time)
        return len(failed) == 0

    def _log_metrics(
        self, files_loaded: int, files_failed: int, start_time: float
    ) -> None:
        duration = time.time() - start_time
        logger.info(f"METRIC pipeline.bronze_load.duration_seconds={duration:.2f}")
        logger.info(f"METRIC pipeline.bronze_load.files_loaded={files_loaded}")
        logger.info(f"METRIC pipeline.bronze_load.files_failed={files_failed}")
        logger.info(
            f"METRIC pipeline.bronze_load.timestamp={datetime.now(timezone.utc).isoformat()}"
        )
