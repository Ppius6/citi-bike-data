#!/usr/bin/env bash
# Run OpenMetadata ingestion workflows against the Docker Compose stack.
#
# Prerequisites:
#   1. docker compose up -d  (stack healthy)
#   2. .env with DB_PASSWORD, MINIO_*, CLICKHOUSE_ENGINEER_PASSWORD, OPENMETADATA_JWT_TOKEN
#   3. dbt artifacts: cd dbt && dbt docs generate --profiles-dir .
#   4. python scripts/clean_manifest.py
#
# Usage:
#   ./scripts/openmetadata/ingest.sh all
#   ./scripts/openmetadata/ingest.sh postgres
#   ./scripts/openmetadata/ingest.sh dbt-postgres

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "ERROR: .env not found. Copy .env.example and set credentials."
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${OPENMETADATA_JWT_TOKEN:?Set OPENMETADATA_JWT_TOKEN in .env (Settings → Bots → ingestion-bot in OM UI)}"
: "${DB_PASSWORD:?Set DB_PASSWORD in .env}"
: "${POSTGRES_ADMIN_PASSWORD:?Set POSTGRES_ADMIN_PASSWORD in .env}"
: "${CLICKHOUSE_ENGINEER_PASSWORD:?Set CLICKHOUSE_ENGINEER_PASSWORD in .env}"
: "${MINIO_ACCESS_KEY:?Set MINIO_ACCESS_KEY in .env}"
: "${MINIO_SECRET_KEY:?Set MINIO_SECRET_KEY in .env}"

NETWORK="$(docker compose ps -q openmetadata >/dev/null 2>&1 && docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(docker compose ps -q openmetadata)" || true)"
if [[ -z "${NETWORK}" ]]; then
  echo "ERROR: OpenMetadata container not running. Start with: docker compose up -d"
  exit 1
fi

IMAGE="citibike-om-ingestion:1.5.0"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Building ingestion image (connectors pre-installed)..."
  docker build -f openmetadata/Dockerfile.ingestion -t "$IMAGE" openmetadata
fi

run_ingest() {
  local config="$1"
  echo ""
  echo "=== Ingesting: ${config} ==="
  docker run --rm \
    --network "$NETWORK" \
    -e OPENMETADATA_JWT_TOKEN \
    -e DB_PASSWORD \
    -e POSTGRES_ADMIN_PASSWORD \
    -e CLICKHOUSE_ENGINEER_PASSWORD \
    -e MINIO_ACCESS_KEY \
    -e MINIO_SECRET_KEY \
    -v "${ROOT}/openmetadata:/opt/ingestion/config:ro" \
    --entrypoint metadata \
    "$IMAGE" \
    ingest -c "/opt/ingestion/config/${config}"
}

prepare_dbt() {
  if [[ ! -f dbt/target/manifest.json ]]; then
    echo "ERROR: dbt/target/manifest.json missing."
    echo "Run: cd dbt && dbt docs generate --profiles-dir ."
    exit 1
  fi
  docker compose exec -T prefect-worker python /app/scripts/clean_manifest.py
  if [[ ! -f openmetadata/dbt-artifacts/postgres/manifest.json ]]; then
    echo "ERROR: openmetadata/dbt-artifacts/postgres/manifest.json was not created."
    exit 1
  fi
}

case "${1:-all}" in
  postgres)
    run_ingest postgres_ingestion.yaml
    ;;
  clickhouse)
    run_ingest clickhouse_ingestion.yaml
    ;;
  minio)
    run_ingest minio_ingestion.yaml
    ;;
  dbt-postgres)
    prepare_dbt
    run_ingest dbt_postgres_ingestion.yaml
    ;;
  dbt-clickhouse)
    prepare_dbt
    run_ingest dbt_clickhouse_ingestion.yaml
    ;;
  all)
    run_ingest postgres_ingestion.yaml
    run_ingest clickhouse_ingestion.yaml
    run_ingest minio_ingestion.yaml
    prepare_dbt
    run_ingest dbt_postgres_ingestion.yaml
    run_ingest dbt_clickhouse_ingestion.yaml
    echo ""
    echo "All ingestion workflows finished."
    ;;
  *)
    echo "Usage: $0 {all|postgres|clickhouse|minio|dbt-postgres|dbt-clickhouse}"
    exit 1
    ;;
esac
