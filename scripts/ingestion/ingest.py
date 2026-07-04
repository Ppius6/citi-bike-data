import logging
import time
from io import BytesIO
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.config.config import PipelineConfig, SourceConfig

logger = logging.getLogger(__name__)


class Downloader:
    def __init__(self, source: SourceConfig, pipeline: PipelineConfig):
        self.source = source
        self.pipeline = pipeline
        self.pipeline.temp_dir.mkdir(parents=True, exist_ok=True)

    def get_file_list(self) -> list[str]:
        """
        Fetch and parse the S3 XML listing, returning all zip keys
        at or after the configured start_from file.
        """
        for attempt in range(self.source.max_retries):
            try:
                response = requests.get(
                    self.source.base_url,
                    timeout=self.source.request_timeout,
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "xml")
                keys = [
                    key.text
                    for key in soup.find_all("Key")
                    if key.text.startswith("JC") and key.text.endswith(".zip")
                ]

                if self.source.start_from not in keys:
                    logger.warning(
                        f"start_from '{self.source.start_from}' not found in listing."
                    )
                    return []

                start_index = keys.index(self.source.start_from)
                file_list = keys[start_index:]
                logger.info(
                    f"Found {len(file_list)} files starting from '{self.source.start_from}'."
                )
                return file_list

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} to fetch file list failed: {e}")
                if attempt < self.source.max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    raise Exception("Max retries exceeded.")

    def download_to_temp(self, file_name: str) -> Optional[Path]:
        """
        Download a zip file to the temp directory.
        Returns the local path, or None if the download fails after all retries.
        Skips the download if the file already exists in temp.
        """
        zip_path = self.pipeline.temp_dir / file_name

        if zip_path.exists():
            logger.info(f"Already in temp, skipping download: {file_name}")
            return zip_path

        url = f"{self.source.base_url}/{file_name}"

        for attempt in range(self.source.max_retries):
            try:
                with requests.get(
                    url, stream=True, timeout=self.source.request_timeout
                ) as r:
                    r.raise_for_status()
                    with open(zip_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=self.source.chunk_size):
                            f.write(chunk)
                logger.info(f"Downloaded: {file_name}")
                return zip_path

            except Exception as e:
                logger.error(
                    f"Attempt {attempt + 1} to download {file_name} failed: {e}"
                )
                if zip_path.exists():
                    zip_path.unlink()
                if attempt < self.source.max_retries - 1:
                    time.sleep(2**attempt)

        logger.error(f"Max retries exceeded downloading {file_name}.")
        return None

    def to_parquet_buffer(self, zip_path: Path) -> Optional[tuple[BytesIO, str]]:
        """
        Extract the CSV from a zip file, read into a DataFrame,
        convert to Parquet in a BytesIO buffer, then clean up temp files.

        Returns a (buffer, suggested_key) tuple, or None if processing fails.
        The key follows the pattern: bronze/<stem>.parquet
        """
        csv_path = None

        try:
            with ZipFile(zip_path, "r") as zf:
                csv_name = next(
                    name
                    for name in zf.namelist()
                    if name.endswith(".csv") and not name.startswith("__MACOSX")
                )
                zf.extract(csv_name, self.pipeline.temp_dir)
                csv_path = self.pipeline.temp_dir / csv_name

            logger.info(f"Extracted: {csv_name}")

            df = self._read_csv(csv_path)

            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")

            key = f"bronze/{zip_path.stem}.parquet"
            logger.info(f"Converted to Parquet buffer: {key} ({len(df)} rows)")

            return buffer, key

        except StopIteration:
            logger.error(f"No CSV found inside {zip_path.name}")
            return None

        except Exception as e:
            logger.error(f"Failed to process {zip_path.name}: {e}")
            return None

        finally:
            if csv_path and csv_path.exists():
                csv_path.unlink()
            if zip_path.exists():
                zip_path.unlink()

    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Read a raw CSV into a DataFrame.
        Applies only the minimum necessary typing — timezone conversion
        and cleaning belong in the silver layer (dbt).
        """
        # keep_default_na=False + na_values=[""]: pandas' default na_values
        # list also treats literal strings like "NA", "NULL", "N/A", "none"
        # as missing — silently destroying real station names/IDs that
        # happen to match one of those tokens. Narrow the coercion to
        # exactly one deliberate rule: empty fields become NULL, everything
        # else survives verbatim.
        df = pd.read_csv(file_path, low_memory=False, keep_default_na=False, na_values=[""])

        for col in ["started_at", "ended_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="mixed", utc=True)

        df["_source_file"] = file_path.name
        df["_ingested_at"] = pd.Timestamp.now(tz="UTC")

        logger.info(f"Read {len(df)} rows from {file_path.name}")
        return df
