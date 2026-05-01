# Citi Bike Data Pipeline

A production-grade lakehouse pipeline that ingests Citi Bike trip data from S3, transforms it through bronze, silver, and gold layers, and serves a star schema for analytical queries. Fully orchestrated with Prefect and containerised with Docker Compose.

---

## Architecture

```
Citi Bike S3 (source)
        ↓
[Prefect — monthly schedule]
        ↓
ingest.py → MinIO              raw Parquet files (bronze layer)
        ↓
loader.py → Postgres           bronze.trips (5.4M rows)
        ↓
dbt-postgres:
  bronze.bronze_trips          exact copy of source, typed
  silver.silver_trips          cleaned, deduplicated, timezone-corrected
  snapshots.station_snapshot   SCD Type 2 station history
        ↓
dbt-clickhouse:
  gold.dim_date                date dimension (date spine)
  gold.dim_station             station dimension with SCD Type 2
  gold.dim_rider_type          rider type dimension
  gold.dim_bike_type           bike type dimension
  gold.fact_trips              fact table, star schema (4.7M rows)
        ↓
DBeaver                        connects to Postgres + ClickHouse
```

---

## Stack

| Layer | Tool | Role |
|---|---|---|
| Source | Citi Bike S3 | Public trip data, monthly zip files |
| Data Lake | MinIO | Local S3-compatible object storage |
| Operational DB | Postgres 16 | Bronze + silver layers, snapshots |
| Warehouse | ClickHouse 24.3 | Gold layer, columnar analytical queries |
| Transformation | dbt (postgres + clickhouse) | Bronze → silver → gold models |
| Orchestration | Prefect 3 | Monthly schedule, task retries, UI |
| Containerisation | Docker Compose | Full stack, single command startup |

---

## Project Structure

```
citi-bike-data/
├── citibike/                  source package
│   ├── config.py              dataclass-based config, env var driven
│   ├── ingest.py              S3 fetch, zip extract, Parquet conversion
│   ├── storage.py             MinIO client wrapper
│   ├── loader.py              MinIO → Postgres bronze loader
│   ├── pipeline.py            ingest + load orchestration entry point
│   └── flows.py               Prefect flow and task definitions
├── dbt/
│   └── citibike_dbt/
│       ├── models/
│       │   ├── bronze/        bronze_trips.sql
│       │   ├── silver/        silver_trips.sql (incremental + merge)
│       │   └── gold/          dim_date, dim_station, dim_rider_type,
│       │                      dim_bike_type, fact_trips
│       ├── snapshots/         station_snapshot.sql (SCD Type 2)
│       ├── macros/            generate_schema_name.sql
│       ├── dbt_project.yml
│       └── profiles.yml
├── clickhouse/
│   ├── config.xml
│   └── users.xml
├── tests/
│   ├── conftest.py
│   ├── test_ingest.py
│   └── test_storage.py
│   └── test_loader.py
├── docker-compose.yml
├── Dockerfile
├── prefect.yaml
└── requirements.txt
```

---

## Prerequisites

- Docker Desktop
- Python 3.11+
- dbt-core, dbt-postgres, dbt-clickhouse
- Prefect 3

---

## Quickstart

**1. Clone and configure environment**

```bash
git clone <repo>
cd citi-bike-data
cp .env.example .env
# Edit .env with your credentials
```

**2. Start the full stack**

```bash
docker compose up -d
```

Services started:

- MinIO console → <http://localhost:9001>
- Prefect UI → <http://localhost:4200>
- Postgres → localhost:5432
- ClickHouse → localhost:8123 (HTTP), localhost:9009 (native)

**3. Run the pipeline manually**

```bash
prefect deployment run 'citibike_pipeline/citibike-monthly'
```

Watch progress at <http://localhost:4200>

---

## Environment Variables

Create a `.env` file at the project root:

```bash
# Postgres
DB_NAME=citi-bike
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=postgres

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=citibike

# ClickHouse
CLICKHOUSE_HOST=clickhouse
```

---

## Data Layers

### Bronze

Raw Citi Bike trip data landed from MinIO Parquet files into Postgres with no transformations. Two metadata columns added: `_source_file` and `_ingested_at`.

### Silver

Cleaned and typed bronze data. Transformations applied:

- Duplicates removed on `ride_id`
- Timestamps converted from UTC to `America/New_York` to align with local ride times and handle UTC offsets correctly.
- `ride_duration_minutes` computed
- Station names and IDs cleaned (`NULLIF` on empty strings)
- Invalid rides filtered (duration ≤ 0 or > 1440 minutes, missing coordinates)

### Gold (Star Schema)

Business-ready dimensional model in ClickHouse:

| Model | Rows | Description |
|---|---|---|
| `dim_date` | ~1,917 | Date spine from 2021-01-01 to latest data |
| `dim_station` | 927 | Stations with SCD Type 2 history |
| `dim_rider_type` | 2 | Member / casual |
| `dim_bike_type` | 3 | Electric / classic / docked |
| `fact_trips` | ~4.7M | One row per ride, FK to all dimensions |

---

## dbt Tests

23 data tests across all layers:

```bash
# Postgres layers (bronze + silver)
dbt test --target dev --select bronze_trips silver_trips --profiles-dir .

# ClickHouse layers (gold)
dbt test --target clickhouse --select gold.* --profiles-dir .
```

---

## Orchestration

The pipeline runs on the 1st of every month at 06:00 AM (New York time).

```
Task execution order:
1. ingest          download + convert to Parquet → MinIO
2. bronze_load     MinIO → Postgres bronze.trips
3. dbt_bronze      bronze.trips → bronze.bronze_trips
4. dbt_silver      bronze_trips → silver.silver_trips (incremental)
5. dbt_snapshot    silver_trips → snapshots.station_snapshot (SCD Type 2)
6. dbt_gold        silver → ClickHouse gold layer (5 models)
7. dbt_test_dev    12 tests on bronze + silver
8. dbt_test_ch     11 tests on gold
```

Each task has automatic retries. A failure in any task stops the flow from proceeding — ensuring no downstream layer is built on bad data.

---

## DBeaver Connections

**Postgres (bronze + silver)**

| Field | Value |
|---|---|
| Host | localhost |
| Port | 5432 |
| Database | citi-bike |
| User | postgres |

**ClickHouse (gold)**

| Field | Value |
|---|---|
| Host | localhost |
| Port | 9009 |
| Database | gold |
| User | default |
| Password | (blank) |

---

## Running Tests

```bash
pytest tests/ -v
```

23 unit tests covering ingest, storage, and loader modules.

---

## Key Design Decisions

**Idempotency.** Every layer checks before writing. The pipeline is safe to re-run at any time and already-processed files are skipped at every stage.

**Medallion architecture.** Bronze is immutable. Silver is replayable from bronze. Gold is replayable from silver. A bug at any layer can be fixed and replayed without re-ingesting from source.

**SCD Type 2 on stations.** Station names and coordinates change over time. dbt snapshots track the full history and each fact row can be joined to the station name that was current at ride time.

**ClickHouse bridge tables.** Gold models read from Postgres silver via ClickHouse's PostgreSQL engine. No data is copied and ClickHouse queries Postgres directly. Only the gold layer is physically stored in ClickHouse.
