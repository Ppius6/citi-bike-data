import logging
from io import BytesIO

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from citibike.config import PostgresConfig
from citibike.storage import MinIOClient

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
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """
        Create the bronze schema if it does not exist.
        """
        with self.engine.begin() as conn:
            conn.execute(
                text(f"CREATE SCHEMA IF NOT EXISTS {self.postgres.bronze_schema}")
            )
        logger.info(f"Schema '{self.postgres.bronze_schema}' is ready.")

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
        read into a DataFrame, and append to bronze.trips.

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

            for i in range(0, row_count, self.postgres.chunk_size):
                chunk = df.iloc[i : i + self.postgres.chunk_size]
                chunk.to_sql(
                    name="trips",
                    schema=self.postgres.bronze_schema,
                    con=self.engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                logger.info(
                    f"Loaded {min(i + self.postgres.chunk_size, row_count)}"
                    f"/{row_count} rows from {key}"
                )

            logger.info(f"Successfully loaded {row_count} rows from {key}")
            return True

        except Exception as e:
            logger.error(f"Failed to load {key}: {e}")
            return False

    def run(self) -> bool:
        """
        Load all Parquet files from the bronze MinIO prefix into
        Postgres bronze.trips, skipping files already loaded.

        Returns:
            bool: True if all files loaded successfully.
        """
        all_keys = self.minio.list_objects(prefix="bronze/")

        if not all_keys:
            logger.info("No Parquet files found in MiniO bronze/.")
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
            return True

        logger.info(f"{len(new_keys)} new Parquet file(s) to load.")

        failed = []
        for key in new_keys:
            success = self.load_parquet(key)
            if not success:
                failed.append(key)

        logger.info(
            f"Bronze load complete. "
            f"Loaded: {len(new_keys) - len(failed)} | failed: {len(failed)}"
        )

        if failed:
            logger.warning(f"Failed keys: {failed}")

        return len(failed) == 0
