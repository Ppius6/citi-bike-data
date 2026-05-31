import io
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest

from scripts.config.config import Config, MinIOConfig, PipelineConfig, SourceConfig


@pytest.fixture
def source_config():
    return SourceConfig(
        base_url="http://example.com",
        start_from="JC-202102-citibike-tripdata.csv.zip",
        chunk_size=1024,
        max_retries=2,
        request_timeout=5,
    )


@pytest.fixture
def pipeline_config(tmp_path):
    return PipelineConfig(
        max_workers=1,
        temp_dir=tmp_path / "temp",
        timezone="America/New_York",
    )


@pytest.fixture
def minio_config():
    return MinIOConfig(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-citibike",
        use_ssl=False,
    )


@pytest.fixture
def config(source_config, pipeline_config, minio_config):
    cfg = Config()
    cfg.source = source_config
    cfg.pipeline = pipeline_config
    cfg.minio = minio_config
    return cfg


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame(
        {
            "ride_id": ["abc123", "def456", "ghi789"],
            "rideable_type": ["electric_bike", "classic_bike", "electric_bike"],
            "started_at": pd.to_datetime(
                [
                    "2021-02-01 08:00:00+00:00",
                    "2021-02-01 09:00:00+00:00",
                    "2021-02-01 10:00:00+00:00",
                ]
            ),
            "ended_at": pd.to_datetime(
                [
                    "2021-02-01 08:30:00+00:00",
                    "2021-02-01 09:45:00+00:00",
                    "2021-02-01 10:20:00+00:00",
                ]
            ),
            "start_station_name": ["Station A", "Station B", "Station C"],
            "end_station_name": ["Station B", "Station C", "Station A"],
            "member_casual": ["member", "casual", "member"],
        }
    )


@pytest.fixture
def sample_zip(tmp_path, sample_dataframe):
    """
    Creates a real zip file containing a CSV derived from sample_dataframe.
    Returns the path to the zip file.
    """
    csv_path = tmp_path / "JC-202102-citibike-tripdata.csv"
    sample_dataframe.to_csv(csv_path, index=False)

    zip_path = tmp_path / "JC-202102-citibike-tripdata.csv.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(csv_path, arcname=csv_path.name)

    return zip_path


@pytest.fixture
def parquet_buffer(sample_dataframe):
    """
    Returns a BytesIO buffer containing sample_dataframe serialised as Parquet.
    """
    buffer = BytesIO()
    sample_dataframe.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    return buffer
