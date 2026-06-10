"""Run OpenMetadata ingestion workflows from the Prefect worker container.

The OpenMetadata ingestion framework pins dependency versions (SQLAlchemy 1.4,
protobuf 4, ...) that conflict with the Prefect/dbt stack, so ingestion runs
in the separate `citibike-om-ingestion` image (openmetadata/Dockerfile.ingestion).
This module launches that image via the Docker socket mounted into the
prefect-worker container (Docker-outside-of-Docker), the same way
scripts/openmetadata/ingest.sh does from the host.
"""
import os
from pathlib import Path

import docker

IMAGE = "citibike-om-ingestion:1.5.0"
CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "openmetadata"

# Forwarded into the ingestion container so `${VAR}` references in the
# openmetadata/*.yaml configs resolve.
PASSTHROUGH_ENV = (
    "OPENMETADATA_JWT_TOKEN",
    "POSTGRES_ADMIN_PASSWORD",
    "CLICKHOUSE_ENGINEER_PASSWORD",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
)


def _network_name(client: docker.DockerClient) -> str:
    """Docker network this container is on, so the ingestion container can
    reach postgres/clickhouse/openmetadata/minio by service name."""
    container = client.containers.get(os.environ["HOSTNAME"])
    return next(iter(container.attrs["NetworkSettings"]["Networks"]))


def _ensure_image(client: docker.DockerClient) -> None:
    try:
        client.images.get(IMAGE)
    except docker.errors.ImageNotFound:
        client.images.build(
            path=str(CONFIG_DIR), dockerfile="Dockerfile.ingestion", tag=IMAGE
        )


def run_ingestion(config_name: str) -> str:
    """Run `metadata ingest -c <config_name>` and return its log output.

    Raises docker.errors.ContainerError if the ingestion exits non-zero.
    """
    client = docker.from_env()
    _ensure_image(client)

    # Bind mounts created via the Docker socket are resolved by the host
    # daemon, so they need the host path, not this container's /app path.
    host_config_dir = f"{os.environ['PROJECT_ROOT']}/openmetadata"
    environment = {key: os.environ[key] for key in PASSTHROUGH_ENV if key in os.environ}

    output = client.containers.run(
        IMAGE,
        command=["ingest", "-c", f"/opt/ingestion/config/{config_name}"],
        entrypoint="metadata",
        network=_network_name(client),
        environment=environment,
        volumes={host_config_dir: {"bind": "/opt/ingestion/config", "mode": "ro"}},
        remove=True,
    )
    return output.decode()
