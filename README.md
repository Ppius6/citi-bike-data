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
OpenMetadata                   data catalog, lineage, dbt docs
        ↓
agent/backend (FastAPI)        DeepSeek tool-calling agent, read-only ai_agent
                                ClickHouse user scoped to gold.* only
        ↓
agent/frontend (React + TS)    minimal chat UI → http://localhost:3000
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
| Data Catalog | OpenMetadata 1.5 | Schema catalog, dbt docs, lineage, search (Elasticsearch) |
| Chat Agent | FastAPI + DeepSeek (OpenAI-compatible) | Tool-calling agent that writes/executes read-only SQL against the gold layer |
| Chat UI | React + Vite + TypeScript | Minimal dark/light chat frontend (Inter/Outfit/JetBrains Mono), served via nginx |
| Containerisation | Docker Compose | Full stack, single command startup |

---

## Project Structure

```
citi-bike-data/
├── scripts/
│   ├── config/                 dataclass-based config, env var driven
│   ├── ingestion/               S3 fetch, zip extract, Parquet conversion
│   ├── storage/                 MinIO client wrapper
│   ├── loading/                 MinIO → Postgres bronze loader
│   ├── quality/                 Soda data quality checks
│   ├── orchestration/           Prefect flow and task definitions
│   ├── openmetadata/             OpenMetadata ingestion runner (ingest.sh)
│   ├── sql/                      Postgres init: schemas + roles
│   ├── minio/                    MinIO bucket + policy init
│   ├── clean_manifest.py        dbt manifest splitter for OpenMetadata
│   └── pipeline.py               ingest + load orchestration entry point
├── dbt/
│   ├── models/
│   │   ├── bronze/             bronze_trips.sql
│   │   ├── silver/             silver_trips.sql (incremental + merge)
│   │   └── gold/                dim_date, dim_station, dim_rider_type,
│   │                            dim_bike_type, fact_trips
│   ├── snapshots/               station_snapshot.sql (SCD Type 2)
│   ├── macros/
│   ├── dbt_project.yml
│   └── profiles.yml
├── openmetadata/                 ingestion configs (postgres, clickhouse, minio, dbt)
├── agent/
│   ├── backend/                 FastAPI + DeepSeek tool-calling agent
│   │   ├── database.py          live gold.* schema introspection + read-only SQL execution/guardrails
│   │   ├── agent.py             system instructions, tool loop, DeepSeek client
│   │   ├── server.py            FastAPI app — POST /api/chat
│   │   ├── main.py              standalone CLI chat loop
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── frontend/                React + Vite + TypeScript chat UI
│       ├── src/
│       │   ├── App.tsx          chat state, message list, composer, dark/light theme toggle
│       │   └── markdownLite.ts  safe **bold**/bullet renderer for agent replies
│       ├── nginx.conf           serves the build, proxies /api → agent-api
│       └── Dockerfile
├── clickhouse/
│   ├── config.xml
│   ├── users.xml
│   └── init.sh                   creates data_engineer / data_analyst / ai_agent users
├── minio/policies/                analyst + engineer access policies
├── docs/
│   └── data_register.md
├── tests/
│   ├── conftest.py
│   ├── test_ingest.py
│   ├── test_storage.py
│   └── test_loader.py
├── docker-compose.yml
├── Dockerfile
├── prefect.yaml
└── pyproject.toml
```

---

## Prerequisites

- Docker Desktop
- Python 3.12+
- Node.js 22+ (only needed for local frontend dev outside Docker)
- dbt-core, dbt-postgres, dbt-clickhouse
- Prefect 3
- A DeepSeek API key (for the chat agent)

---

## Quickstart

**1. Clone and configure environment**

```bash
git clone <repo>
cd citi-bike-data
# Create a .env file with the variables listed below
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
- OpenMetadata UI → <http://localhost:8585>
- Chat agent API → <http://localhost:8000> (see [Chat Agent](#chat-agent))
- Chat UI → <http://localhost:3000>

**3. Run the pipeline manually**

```bash
prefect deployment run 'citibike_pipeline/citibike-monthly'
```

Watch progress at <http://localhost:4200>

**4. Ingest metadata into OpenMetadata (optional)**

```bash
./scripts/openmetadata/ingest.sh all
```

Browse the catalog, lineage, and dbt docs at <http://localhost:8585>

---

## Environment Variables

Create a `.env` file at the project root:

```bash
# Postgres
DB_NAME=citi-bike
DB_HOST=postgres
POSTGRES_ADMIN_PASSWORD=your_admin_password
DB_PASSWORD=your_data_engineer_password
ANALYST_PASSWORD=your_data_analyst_password

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=citibike
MINIO_ACCESS_KEY=your_engineer_access_key
MINIO_SECRET_KEY=your_engineer_secret_key
MINIO_ANALYST_KEY=your_analyst_access_key
MINIO_ANALYST_SECRET=your_analyst_secret_key

# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_ENGINEER_PASSWORD=your_clickhouse_password

# ClickHouse — read-only chat agent user (SELECT on gold.* only, server-enforced readonly=2)
AI_AGENT_PASSWORD=your_ai_agent_password

# DeepSeek (chat agent LLM)
DEEPSEEK_API_KEY=your_deepseek_api_key

# OpenMetadata (Settings → Bots → ingestion-bot in OM UI)
OPENMETADATA_JWT_TOKEN=your_jwt_token
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
1.  ingest              download + convert to Parquet → MinIO
2.  bronze_load         MinIO → Postgres bronze.trips
3.  quality_check       Soda checks on bronze.trips
4.  dbt_bronze          bronze.trips → bronze.bronze_trips
5.  dbt_silver          bronze_trips → silver.silver_trips (incremental)
6.  dbt_elementary      Elementary monitoring models in silver
7.  dbt_snapshot        silver_trips → snapshots.station_snapshot (SCD Type 2)
8.  dbt_gold            silver → ClickHouse gold layer (5 models)
9.  dbt_test_dev        12 tests on bronze + silver
10. dbt_test_ch         11 tests on gold
11. om_ingest_postgres  refresh OpenMetadata: Postgres schemas
12. om_ingest_clickhouse refresh OpenMetadata: ClickHouse schemas
13. om_ingest_minio     refresh OpenMetadata: MinIO storage
14. dbt_docs_generate   regenerate dbt manifest/catalog
15. clean_dbt_manifest  split manifest into Postgres + ClickHouse artifacts
16. om_ingest_dbt_*     refresh OpenMetadata: dbt docs + lineage
```

Each pipeline task (1–10) has automatic retries, and a failure stops the flow before any downstream layer is built on bad data. The catalog refresh tasks (11–16) are best-effort: failures are logged but won't fail the run, since a stale catalog shouldn't block the data pipeline.

![Completed pipeline run in the Prefect UI](files/pipeline.png)

An example of the orchestration is viewable in the following image:

![Orchestration Flow](files/pipeline.png)

---

## Data Catalog (OpenMetadata)

OpenMetadata indexes the Postgres (bronze/silver/snapshots) and ClickHouse (gold) schemas, plus dbt model docs, descriptions, tests, and column-level lineage between the two databases.

The catalog refreshes automatically as the last stage of `citibike_pipeline` (see [Orchestration](#orchestration)). The `prefect-worker` container launches a short-lived `citibike-om-ingestion` container per workflow via the Docker socket — kept separate because OpenMetadata's ingestion framework pins dependency versions (SQLAlchemy 1.4, protobuf 4) that conflict with Prefect/dbt's. This requires:

- `/var/run/docker.sock` mounted into `prefect-worker` (already set in `docker-compose.yml`)
- `PROJECT_ROOT` set to the project's host path (defaults to `${PWD}` — run `docker compose up` from the project root)

To run ingestion manually instead:

```bash
# Generate fresh dbt artifacts, then ingest everything
cd dbt && dbt docs generate --profiles-dir . && cd ..
./scripts/openmetadata/ingest.sh all

# Or run a single workflow
./scripts/openmetadata/ingest.sh dbt-postgres
```

Open <http://localhost:8585> and sign in with the admin credentials configured on first run. The ingestion workflows are defined in `openmetadata/*.yaml`; `scripts/clean_manifest.py` splits the dbt manifest into Postgres and ClickHouse artifacts compatible with OpenMetadata 1.5's dbt manifest schema.

---

## Chat Agent

A minimal chat UI for asking questions about the gold layer in plain English — "What was the average ride duration for casual riders versus members?" — backed by a tool-calling agent that writes and executes the SQL itself. Dark by default with a light-mode toggle, persisted to `localStorage`.

| Light | Dark |
|---|---|
| ![Chat agent, light theme](files/agent.png) | ![Chat agent, dark theme](files/agent-2.png) |

```
web browser → agent/frontend (React + TS, nginx)
                    ↓ /api/chat (proxied)
              agent/backend (FastAPI)
                    ↓ DeepSeek tool-calling loop
              gold.* (ClickHouse, ai_agent user)
```

**How it works:**

1. At startup, `agent/backend/database.py` introspects `system.columns` for `gold.*` live (via the same read-only `ai_agent` user) and probes the actual `DISTINCT` values of the rider/bike-type dimension columns. This becomes the schema context baked into the system prompt — it reflects the real dbt models, not a hand-maintained description that can drift when a model changes.
2. `agent/backend/agent.py` sends the user's question to DeepSeek along with that schema context and the join keys between `fact_trips` and its dimensions.
3. DeepSeek responds with a tool call containing a generated SQL query. The loop keeps `tools` available on every turn — required for DeepSeek's function-calling to behave correctly across multiple rounds.
4. `agent/backend/database.py` executes that query against ClickHouse and returns the rows.
5. DeepSeek reads the results and writes the final plain-English answer, which the UI shows along with a collapsible "SQL used" section.

**Guardrails** (defense in depth — each layer works even if another fails):

- App layer: only a single `SELECT`/`WITH` statement is allowed per call; a keyword block-list rejects `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `CREATE`, and other mutating statements.
- Database layer: the agent connects as a dedicated `ai_agent` ClickHouse user (see `clickhouse/init.sh`) that is granted `SELECT` on `gold.*` only — no access to `silver`/`snapshots`/`bronze` — and created with `SETTINGS readonly = 2`, which makes ClickHouse itself reject any write statement regardless of what the app layer does.
- Correctness: ClickHouse string comparisons are case-sensitive, and dimension tables store both a raw value (`rider_type = 'casual'`) and a display value (`rider_type_desc = 'Casual'`). The system prompt instructs the agent to filter with `lower(column) = lower('value')` unless it's certain of exact casing, so a wrong guess returns the right rows instead of silently returning zero.

**Run standalone (CLI, no Docker):**

```bash
cd agent/backend
pip install -r requirements.txt
python main.py
```

**Run via Docker Compose** (part of `docker compose up -d`, see [Quickstart](#quickstart)): the `agent-api` service builds from `agent/backend/`, and `web` builds from `agent/frontend/` and serves the UI at <http://localhost:3000>, with nginx proxying `/api/*` to `agent-api`.

---

The UI of the agent is as shown below.

![Chat Agent UI](files/agent.png)

![Chat Agent UI](files/agent-2.png)

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
