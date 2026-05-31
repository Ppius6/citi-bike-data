import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pandas as pd
import pytest
import requests

from scripts.ingest import Downloader


@pytest.fixture
def downloader(source_config, pipeline_config):
    return Downloader(source_config, pipeline_config)


class TestGetFileList:
    def test_returns_files_from_start_from(self, downloader):
        xml = """<?xml version="1.0"?>
        <ListBucketResult>
            <Key>JC-202101-citibike-tripdata.csv.zip</Key>
            <Key>JC-202102-citibike-tripdata.csv.zip</Key>
            <Key>JC-202103-citibike-tripdata.csv.zip</Key>
        </ListBucketResult>"""

        with patch("citibike.ingest.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.text = xml
            mock_get.return_value.raise_for_status = MagicMock()

            result = downloader.get_file_list()

        assert result == [
            "JC-202102-citibike-tripdata.csv.zip",
            "JC-202103-citibike-tripdata.csv.zip",
        ]

    def test_returns_empty_list_when_start_from_not_found(self, downloader):
        xml = """<?xml version="1.0"?>
        <ListBucketResult>
            <Key>JC-202001-citibike-tripdata.csv.zip</Key>
        </ListBucketResult>"""

        with patch("citibike.ingest.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.text = xml
            mock_get.return_value.raise_for_status = MagicMock()

            result = downloader.get_file_list()

        assert result == []

    def test_retries_on_failure_then_raises(self, downloader):
        with patch("citibike.ingest.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.ConnectionError("timeout")

            with pytest.raises(Exception, match="Max retries exceeded"):
                downloader.get_file_list()

        assert mock_get.call_count == downloader.source.max_retries


class TestDownloadToTemp:
    def test_downloads_file_successfully(self, downloader, tmp_path):
        file_name = "JC-202102-citibike-tripdata.csv.zip"
        fake_content = b"fake zip content"

        with patch("citibike.ingest.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.__enter__ = lambda s: s
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_response.iter_content.return_value = [fake_content]
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            result = downloader.download_to_temp(file_name)

        assert result is not None
        assert result.exists()
        assert result.read_bytes() == fake_content

    def test_skips_download_if_already_in_temp(self, downloader):
        file_name = "JC-202102-citibike-tripdata.csv.zip"
        existing = downloader.pipeline.temp_dir / file_name
        downloader.pipeline.temp_dir.mkdir(parents=True, exist_ok=True)
        existing.write_bytes(b"already here")

        with patch("citibike.ingest.requests.get") as mock_get:
            result = downloader.download_to_temp(file_name)
            mock_get.assert_not_called()

        assert result == existing

    def test_returns_none_and_cleans_up_on_failure(self, downloader):
        file_name = "JC-202102-citibike-tripdata.csv.zip"

        with patch("citibike.ingest.requests.get") as mock_get:
            mock_get.side_effect = Exception("network error")
            result = downloader.download_to_temp(file_name)

        assert result is None
        assert not (downloader.pipeline.temp_dir / file_name).exists()


class TestToParquetBuffer:
    def test_returns_buffer_and_key(self, downloader, sample_zip):
        result = downloader.to_parquet_buffer(sample_zip)

        assert result is not None
        buffer, key = result
        assert isinstance(buffer, BytesIO)
        assert key.startswith("bronze/")
        assert key.endswith(".parquet")

    def test_cleans_up_zip_and_csv_after_processing(self, downloader, sample_zip):
        downloader.to_parquet_buffer(sample_zip)
        assert not sample_zip.exists()

    def test_returns_none_when_no_csv_in_zip(self, downloader, tmp_path):
        zip_path = tmp_path / "empty.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("notes.txt", "not a csv")

        result = downloader.to_parquet_buffer(zip_path)
        assert result is None

    def test_parquet_buffer_is_readable(self, downloader, sample_zip):
        buffer, _ = downloader.to_parquet_buffer(sample_zip)
        df = pd.read_parquet(buffer)
        assert len(df) > 0


class TestReadCsv:
    def test_adds_metadata_columns(self, downloader, tmp_path, sample_dataframe):
        csv_path = tmp_path / "test.csv"
        sample_dataframe.to_csv(csv_path, index=False)

        df = downloader._read_csv(csv_path)

        assert "_source_file" in df.columns
        assert "_ingested_at" in df.columns
        assert df["_source_file"].iloc[0] == "test.csv"

    def test_parses_date_columns_to_utc(self, downloader, tmp_path, sample_dataframe):
        csv_path = tmp_path / "test.csv"
        sample_dataframe.to_csv(csv_path, index=False)

        df = downloader._read_csv(csv_path)

        assert df["started_at"].dt.tz is not None
        assert str(df["started_at"].dt.tz) == "UTC"

    def test_row_count_matches_source(self, downloader, tmp_path, sample_dataframe):
        csv_path = tmp_path / "test.csv"
        sample_dataframe.to_csv(csv_path, index=False)

        df = downloader._read_csv(csv_path)

        assert len(df) == len(sample_dataframe)
