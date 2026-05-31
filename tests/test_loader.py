import pandas as pd
import pytest
from io import BytesIO
from unittest.mock import MagicMock, patch, PropertyMock
from sqlalchemy.exc import SQLAlchemyError

from scripts.config import PostgresConfig
from scripts.loader import BronzeLoader


@pytest.fixture
def mock_engine():
    with patch("citibike.loader.create_engine") as mock:
        engine = MagicMock()
        mock.return_value = engine
        yield engine


@pytest.fixture
def mock_minio(parquet_buffer, sample_dataframe):
    """
    Returns a mock MinIOClient that simulates listing and downloading
    Parquet files from MinIO.
    """
    minio = MagicMock()
    minio.config.bucket = "test-citibike"
    minio.list_objects.return_value = [
        "bronze/JC-202102-citibike-tripdata.csv.parquet",
        "bronze/JC-202103-citibike-tripdata.csv.parquet",
    ]

    parquet_buffer.seek(0)
    minio.client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=parquet_buffer.read()))
    }

    return minio


@pytest.fixture
def loader(postgres_config, mock_minio, mock_engine):
    return BronzeLoader(postgres_config, mock_minio)


@pytest.fixture
def postgres_config():
    return PostgresConfig(
        host="localhost",
        port=5432,
        name="citibike_test",
        user="postgres",
        password="postgres",
        bronze_schema="bronze",
        chunk_size=2,
    )


class TestEnsureSchema:
    def test_creates_schema_on_init(self, postgres_config, mock_minio, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        BronzeLoader(postgres_config, mock_minio)
        conn.execute.assert_called_once()
        call_text = str(conn.execute.call_args[0][0])
        assert "CREATE SCHEMA IF NOT EXISTS" in call_text


class TestGetLoadedSources:
    def test_returns_set_of_source_files(self, loader, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value = [
            ("JC-202102-citibike-tripdata.csv",),
            ("JC-202103-citibike-tripdata.csv",),
        ]
        result = loader.get_loaded_sources()
        assert result == {
            "JC-202102-citibike-tripdata.csv",
            "JC-202103-citibike-tripdata.csv",
        }

    def test_returns_empty_set_when_table_missing(self, loader, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.side_effect = SQLAlchemyError("table does not exist")
        result = loader.get_loaded_sources()
        assert result == set()


class TestLoadParquet:
    def test_loads_parquet_successfully(self, loader, mock_engine, parquet_buffer):
        parquet_buffer.seek(0)
        loader.minio.client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=parquet_buffer.read()))
        }

        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            result = loader.load_parquet(
                "bronze/JC-202102-citibike-tripdata.csv.parquet"
            )

        assert result is True

    def test_returns_false_on_failure(self, loader):
        loader.minio.client.get_object.side_effect = Exception("connection error")
        result = loader.load_parquet("bronze/missing.parquet")
        assert result is False

    def test_skips_empty_parquet(self, loader, mock_engine):
        empty_buffer = BytesIO()
        pd.DataFrame().to_parquet(empty_buffer, engine="pyarrow")
        empty_buffer.seek(0)

        loader.minio.client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=empty_buffer.read()))
        }

        result = loader.load_parquet("bronze/empty.parquet")
        assert result is True


class TestRun:
    def test_loads_only_new_files(self, loader, mock_engine):
        loader.minio.list_objects.return_value = [
            "bronze/JC-202102-citibike-tripdata.csv.parquet",
            "bronze/JC-202103-citibike-tripdata.csv.parquet",
        ]

        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value = [
            ("JC-202102-citibike-tripdata.csv",),
        ]

        with patch.object(loader, "load_parquet", return_value=True) as mock_load:
            loader.run()

        assert mock_load.call_count == 1
        mock_load.assert_called_once_with(
            "bronze/JC-202103-citibike-tripdata.csv.parquet"
        )

    def test_returns_true_when_nothing_to_load(self, loader, mock_engine):
        loader.minio.list_objects.return_value = [
            "bronze/JC-202102-citibike-tripdata.csv.parquet",
        ]

        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value = [
            ("JC-202102-citibike-tripdata.csv",),
        ]

        result = loader.run()
        assert result is True

    def test_returns_false_when_load_fails(self, loader, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value = []

        with patch.object(loader, "load_parquet", return_value=False):
            result = loader.run()

        assert result is False

    def test_returns_true_when_no_files_in_minio(self, loader):
        loader.minio.list_objects.return_value = []
        result = loader.run()
        assert result is True
