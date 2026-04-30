import logging
from io import BytesIO

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from citibike.config import MinIOConfig

logger = logging.getLogger(__name__)


class MinIOClient:
    def __init__(self, config: MinIOConfig):
        self.config = config
        self.client = self._create_client()
        self._ensure_bucket()

    def _create_client(self):
        return boto3.client(
            "s3",
            endpoint_url=f"http{'s' if self.config.use_ssl else ''}://{self.config.endpoint}",
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.config.bucket)
            logger.info(f"Bucket '{self.config.bucket}' already exists.")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                self.client.create_bucket(Bucket=self.config.bucket)
                logger.info(f"Bucket '{self.config.bucket}' created.")
            else:
                raise

    def object_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.config.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def upload_parquet(self, buffer: BytesIO, key: str) -> None:
        buffer.seek(0)
        self.client.put_object(
            Bucket=self.config.bucket,
            Key=key,
            Body=buffer,
            ContentType="application/octet-stream",
        )
        logger.info(f"Uploaded: {key}")

    def list_objects(self, prefix: str = "") -> list[str]:
        response = self.client.list_objects_v2(
            Bucket=self.config.bucket,
            Prefix=prefix,
        )
        return [obj["Key"] for obj in response.get("Contents", [])]
