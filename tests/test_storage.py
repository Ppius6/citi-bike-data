import pytest
from io import BytesIO
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from scripts.storage.minio_client import MinIOClient


def make_client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": ""}}, "operation")


@pytest.fixture
def mock_boto3_client():
    with patch("citibike.storage.minio_client.boto3.client") as mock:
        yield mock


@pytest.fixture
def minio_client(minio_config, mock_boto3_client):
    """
    Returns a MinIOClient with a mocked boto3 client.
    head_bucket is set to succeed by default (bucket already exists).
    """
    mock_boto3_client.return_value.head_bucket.return_value = {}
    return MinIOClient(minio_config)


class TestEnsureBucket:
    def test_does_not_create_bucket_if_already_exists(
        self, minio_config, mock_boto3_client
    ):
        mock_boto3_client.return_value.head_bucket.return_value = {}
        client = MinIOClient(minio_config)
        mock_boto3_client.return_value.create_bucket.assert_not_called()

    def test_creates_bucket_if_not_found(self, minio_config, mock_boto3_client):
        mock_boto3_client.return_value.head_bucket.side_effect = make_client_error(
            "404"
        )
        client = MinIOClient(minio_config)
        mock_boto3_client.return_value.create_bucket.assert_called_once_with(
            Bucket=minio_config.bucket
        )

    def test_raises_on_unexpected_error(self, minio_config, mock_boto3_client):
        mock_boto3_client.return_value.head_bucket.side_effect = make_client_error(
            "403"
        )
        with pytest.raises(ClientError):
            MinIOClient(minio_config)


class TestObjectExists:
    def test_returns_true_when_object_exists(self, minio_client):
        minio_client.client.head_object.return_value = {}
        assert minio_client.object_exists("bronze/some-file.parquet") is True

    def test_returns_false_when_object_not_found(self, minio_client):
        minio_client.client.head_object.side_effect = make_client_error("404")
        assert minio_client.object_exists("bronze/missing-file.parquet") is False

    def test_raises_on_unexpected_error(self, minio_client):
        minio_client.client.head_object.side_effect = make_client_error("403")
        with pytest.raises(ClientError):
            minio_client.object_exists("bronze/some-file.parquet")


class TestUploadParquet:
    def test_uploads_buffer_to_correct_key(self, minio_client, parquet_buffer):
        minio_client.upload_parquet(parquet_buffer, "bronze/test.parquet")
        minio_client.client.put_object.assert_called_once()

        call_kwargs = minio_client.client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == minio_client.config.bucket
        assert call_kwargs["Key"] == "bronze/test.parquet"

    def test_rewinds_buffer_before_upload(self, minio_client, parquet_buffer):
        parquet_buffer.seek(0, 2)  # move cursor to end
        minio_client.upload_parquet(parquet_buffer, "bronze/test.parquet")

        call_kwargs = minio_client.client.put_object.call_args.kwargs
        body = call_kwargs["Body"]
        assert body.tell() == 0  # confirm buffer was rewound


class TestListObjects:
    def test_returns_list_of_keys(self, minio_client):
        minio_client.client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "bronze/file1.parquet"},
                {"Key": "bronze/file2.parquet"},
            ]
        }
        result = minio_client.list_objects(prefix="bronze/")
        assert result == ["bronze/file1.parquet", "bronze/file2.parquet"]

    def test_returns_empty_list_when_no_objects(self, minio_client):
        minio_client.client.list_objects_v2.return_value = {}
        result = minio_client.list_objects(prefix="bronze/")
        assert result == []
