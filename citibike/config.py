import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class MinIOConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket: str = os.getenv("MINIO_BUCKET", "citibike")
    use_ssl: bool = False


@dataclass
class SourceConfig:
    base_url: str = "https://s3.amazonaws.com/tripdata"
    start_from: str = "JC-202102-citibike-tripdata.csv.zip"
    chunk_size: int = 8192
    max_retries: int = 3
    request_timeout: int = 30


@dataclass
class PipelineConfig:
    max_workers: int = 4
    temp_dir: Path = field(
        default_factory=lambda: Path(os.getenv("TEMP_DIR", "/tmp/citibike"))
    )
    timezone: str = "America/New_York"


@dataclass
class PostgresConfig:
    host: str = os.getenv("DB_HOST", "postgres")
    port: int = int(os.getenv("DB_PORT", "5432"))
    name: str = os.getenv("DB_NAME", "citibike")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "postgres")
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    chunk_size: int = 5000
    
    @property
    def url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )

@dataclass
class Config:
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    source: SourceConfig = field(default_factory=SourceConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
