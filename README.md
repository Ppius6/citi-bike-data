# Citi Bike Data Pipeline

A hybrid warehouse pipeline with MinIO as an object-store landing zone, Postgres for the mutable bronze/silver layers, ClickHouse for the gold star schema, that ingests Citi Bike trip data from S3, transforms it through bronze, silver, and gold layers, and serves analytical queries. Fully orchestrated with Prefect and containerised with Docker Compose. (See [Key Design Decisions](#key-design-decisions) for why bronze is Postgres rather than ClickHouse querying Parquet directly.)

---

## Architecture

![Architecture](docs/images/main-architecture.png)

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
| Data Catalog | dbt docs | Model docs, column descriptions, tests, and lineage — live at [ppius6.github.io/citi-bike-data](https://ppius6.github.io/citi-bike-data/) |
| Observability | Soda + dbt tests + Elementary | Landing contract, model assertions, and anomaly/run-history report — live at [.../elementary](https://ppius6.github.io/citi-bike-data/elementary/) |
| Chat Agent | FastAPI + DeepSeek + sentence-transformers | Tool-calling agent that writes read-only SQL against the gold layer, with long-term vector memory |
| Chat UI | React + Vite + TypeScript | Chat frontend, served via nginx |
| Containerisation | Docker Compose | Full stack, single command startup |

---

## Project Structure

```
citi-bike-data/
├── .github/workflows/
│   └── ci.yml                    ruff check, pytest (root + agent), dbt parse
├── scripts/
│   ├── config/                 dataclass-based config, env var driven
│   ├── ingestion/               S3 fetch, zip extract, Parquet conversion
│   ├── storage/                 MinIO client wrapper
│   ├── loading/                 MinIO → Postgres bronze loader (COPY-based)
│   ├── quality/                 Soda data quality checks
│   ├── orchestration/           Prefect flow and task definitions
│   └── pipeline.py               ingest + load orchestration entry point
├── dbt/
│   ├── models/
│   │   ├── bronze/             bronze_trips.sql
│   │   ├── silver/             int_trips_cleaned.sql (incremental, single scan),
│   │   │                        silver_trips.sql / silver_trips_rejected.sql (views)
│   │   └── gold/                dim_date, dim_station, dim_rider_type,
│   │                            dim_bike_type, fact_trips
│   ├── snapshots/               station_snapshot.sql (SCD Type 2)
│   ├── macros/
│   ├── dbt_project.yml
│   └── profiles.yml
├── agent/
│   ├── backend/                 FastAPI + DeepSeek tool-calling agent
│   │   ├── database.py          live gold.* schema introspection + read-only SQL execution/guardrails
│   │   ├── agent.py             system instructions, tool loop, DeepSeek client
│   │   ├── server.py            FastAPI app — POST /api/chat
│   │   ├── main.py              standalone CLI chat loop
│   │   ├── tests/               guardrail tests for execute_sql_query
│   │   ├── requirements.txt
│   │   ├── requirements-dev.txt
│   │   └── Dockerfile
│   └── frontend/                React + Vite + TypeScript chat UI
│       ├── src/
│       │   ├── App.tsx          chat state, message list, composer, dark/light theme toggle
│       │   └── markdownLite.ts  safe **bold**/bullet renderer for agent replies
│       ├── nginx.conf           serves the build, proxies /api → agent-api
│       └── Dockerfile
├── infra/
│   ├── postgres/
│   │   └── init.sh                schemas + roles
│   ├── clickhouse/
│   │   ├── config.xml
│   │   ├── users.xml
│   │   └── init.sh                creates data_engineer / data_analyst / ai_agent users
│   └── minio/
│       ├── init.sh                bucket + policy init
│       └── policies/              analyst + engineer access policies
├── docs/
│   ├── images/                    architecture diagrams, chat agent screenshots
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
cp .env.example .env
# Fill in the values in .env
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
- Chat agent API → <http://localhost:8000> (see [Chat Agent](#chat-agent))
- Chat UI → <http://localhost:3000>

**3. Run the pipeline manually**

```bash
prefect deployment run 'citibike_pipeline/citibike-monthly'
```

Watch progress at <http://localhost:4200> or monitor logs with `docker compose logs -f pipeline`. The pipeline will also run automatically on the 1st of every month at 06:00 AM New York time.

**4. Browse the data catalog (dbt docs)**

Live at **<https://ppius6.github.io/citi-bike-data/>** — model docs, column descriptions, tests, and lineage, published automatically by CI on every push to `main` (see `.github/workflows/ci.yml`'s `deploy-docs` job). Elementary's anomaly/run-history report is published alongside it at **<https://ppius6.github.io/citi-bike-data/elementary/>**.

To generate and browse local copies instead (useful while iterating on models before pushing):

```bash
cd dbt && dbt docs generate --profiles-dir . --target dev && dbt docs serve --profiles-dir .

# Elementary report (needs its own `elementary` profile stanza in profiles.yml)
mkdir -p target/elementary
edr report --profiles-dir . --project-dir . --profile-target dev --file-path target/elementary/index.html
```

---

## Environment Variables

```bash
cp .env.example .env
```

`.env.example` is the source of truth for every variable the stack needs — Postgres (admin, `data_engineer`, `data_analyst` passwords), MinIO (root + engineer/analyst keys), ClickHouse (`data_engineer`/`ai_agent` passwords), and the DeepSeek API key for the chat agent. Fill in real values in `.env`; nothing in the repo besides `.env.example` should contain a real secret.

---

## Data Layers

### Bronze

Raw Citi Bike trip data landed from MinIO Parquet files into Postgres with no transformations. Two metadata columns added: `_source_file` and `_ingested_at`.

### Silver

`int_trips_cleaned` is the single incremental scan over bronze — cleaning, dedup, and quarantine flagging all happen here once. `silver_trips` and `silver_trips_rejected` are thin views splitting on `rejection_reason`, so downstream consumers see the same shape as before. Transformations applied:

- Duplicates removed on `ride_id`
- Timestamps converted from UTC to `America/New_York` (single conversion — the raw column is already `timestamptz`)
- `ride_duration_minutes` computed
- Station names and IDs cleaned (`NULLIF` on empty strings); a missing `station_id` with a surviving `station_name` is backfilled from an unambiguous name→id lookup built from the rest of the data — station IDs don't change over time, so a name seen with exactly one other ID elsewhere is safe to fill in
- Invalid rides quarantined, not dropped: duration ≤ 0 or > 1440 minutes, missing coordinates → `silver_trips_rejected`, tagged with why

### Gold (Star Schema)

Business-ready dimensional model in ClickHouse:

| Model | Rows | Description |
|---|---|---|
| `dim_date` | ~1,977 | Date spine from 2021-01-01 to latest data, `date_key` as `YYYYMMDD` `UInt32` |
| `dim_station` | 939 | Stations with SCD Type 2 history, `station_key` unique per version |
| `dim_rider_type` | 2 | Member / casual |
| `dim_bike_type` | 3 | Electric / classic / docked |
| `fact_trips` | ~4.86M | One row per ride, FK to all dimensions, `ORDER BY (date_key, start_station_key)` — rides with no resolvable station are excluded (~0.3%) |

---

## dbt Tests

42 data tests across all layers, including `relationships` tests from every `fact_trips` FK to its dimension and `accepted_values` on the dimension enums:

```bash
# Postgres layers (bronze + silver)
dbt test --target dev --select bronze_trips int_trips_cleaned silver_trips silver_trips_rejected --profiles-dir .

# ClickHouse layers (gold)
dbt test --target clickhouse --select gold.* --profiles-dir .
```

---

## Data Quality, Observability, and Governance

- **Soda** runs data quality checks right after data lands in the bronze layer using Soda Core  integrated into the pipeline using Soda's YAML-based check definitions. It runs immediately after the raw data is loaded into the postgres `bronze.trips` table from MinIO, but before any dbt models start running.

- **dbt tests** checks run on every layer i.e., ensuring primary keys are unique, foreign keys in the fact table actually match the dimension tables, and the enum values are correct. These tests are separated into two groups, `dev` (for postgres layers) and `ch` (for clickhouse layers).

- **Elementary** plugs into dbt to monitor anomalies and report on our pipeline's run history. It runs after the `bronze` and `silver` dbt models have executed.

- **Data Catalog (dbt docs)** automatically generates a searcheable website mapping out of every table, column, and lineage graph (how data flows from source to destination) 

See the dbt docs deployed at <https://ppius6.github.io/citi-bike-data/> and also the elementary report at <https://ppius6.github.io/citi-bike-data/elementary/>.

## Orchestration

The pipeline runs on the 1st of every month at 06:00 AM (New York time), since the source data is published on that cadence.

```
Task execution order:
1.  ingest              download + convert to Parquet → MinIO
2.  bronze_load         MinIO → Postgres bronze.trips (COPY)
3.  quality_check       Soda checks on bronze.trips
4.  dbt_bronze          bronze.trips → bronze.bronze_trips
5.  dbt_silver          bronze_trips → silver.int_trips_cleaned / silver_trips / silver_trips_rejected
6.  dbt_elementary      Elementary monitoring models in silver
7.  dbt_snapshot        silver_trips → snapshots.station_snapshot (SCD Type 2)
8.  dbt_gold            silver → ClickHouse gold layer (5 models)
9.  dbt_test_dev        tests on bronze + silver
10. dbt_test_ch         tests on gold
11. dbt_docs_generate   regenerate dbt docs (model docs, tests, lineage)
12. elementary_report   regenerate Elementary's anomaly/observability report
```

Each task has automatic retries, and a failure stops the flow before any downstream layer is built on bad data.

![Completed pipeline run in the Prefect UI](docs/images/pipeline.png)

---

## Chat Agent

A minimal chat UI for asking questions about the gold layer in plain English — "What was the average ride duration for casual riders versus members?" — backed by a tool-calling agent that writes and executes the SQL itself. Dark by default with a light-mode toggle, persisted to `localStorage`.

| Light | Dark |
|---|---|
| ![Chat agent, light theme](docs/images/agent.png) | ![Chat agent, dark theme](docs/images/agent-2.png) |

### Architecture

![Architecture](docs/images/ai-agent-architecture.png)

**How it works:**

1. **Initialization:** At startup, `agent/backend/database.py` introspects `system.columns` for `gold.*` live (via the same read-only `ai_agent` user) and probes the actual `DISTINCT` values of the rider/bike-type dimension columns. This becomes the schema context baked into the system prompt which reflects the real dbt models, not a hand-maintained description that can drift when a model changes.
2. **Memory Retrieval:** The agent embeds the user's question using a local `sentence-transformers` model (`all-MiniLM-L6-v2`) and searches the `agent.memory` table in ClickHouse using `cosineDistance`. The most semantically similar past questions are retrieved and their corresponding SQL queries are injected into the system prompt as proven few-shot examples.
3. **Query Generation:** `agent/backend/agent.py` sends the user's question and the entire chat history (for conversational context) to DeepSeek along with the schema context and retrieved memory examples.
4. **Execution Loop:** DeepSeek responds with a tool call containing a generated SQL query. The loop keeps `tools` available on every turn which is required for DeepSeek's function-calling to behave correctly across multiple rounds.
5. **Memory Storage:** Once the agent finishes executing exploratory queries and successfully produces the final plain-English answer, the single most successful query is embedded and saved back to the `agent.memory` ClickHouse table for future reference.

**Guardrails** (defense in depth where each layer works even if another fails):

- App layer: only a single `SELECT`/`WITH` statement is allowed per call; a keyword block-list rejects `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `CREATE`, and other mutating statements — including keywords hidden inside a `WITH ... DELETE` CTE. Covered by 29 guardrail tests in `agent/backend/tests/test_database.py` (statement-count enforcement, every forbidden keyword, case-insensitivity, and word-boundary checks so identifiers like `inserted_at` don't false-positive on `INSERT`).
- Database layer: the agent connects as a dedicated `ai_agent` ClickHouse user (see `infra/clickhouse/init.sh`) that is granted `SELECT` on `gold.*` and `INSERT`/`SELECT` on `agent.memory`. It is created without restricted readonly settings to allow it to persist memories, relying completely on explicit table-level RBAC grants to prevent unauthorized writes.
- Correctness: ClickHouse string comparisons are case-sensitive, and dimension tables store both a raw value (`rider_type = 'casual'`) and a display value (`rider_type_desc = 'Casual'`). The system prompt instructs the agent to filter with `lower(column) = lower('value')` unless it's certain of exact casing, so a wrong guess returns the right rows instead of silently returning zero.
- Vector Pollution: The agent tracks its exploratory tool calls and only commits the final, successful query to its long-term memory to prevent memorizing confused exploratory queries.

**Run standalone (CLI, no Docker):**

```bash
cd agent/backend
pip install -r requirements.txt
python main.py
```

**Run via Docker Compose** (part of `docker compose up -d`, see [Quickstart](#quickstart)): the `agent-api` service builds from `agent/backend/`, and `web` builds from `agent/frontend/` and serves the UI at <http://localhost:3000>, with nginx proxying `/api/*` to `agent-api`.

---

## DBeaver Connections

Connect with the least-privilege `data_analyst` role for read-only exploration; use `data_engineer` only if you need to write.

**Postgres (bronze + silver)**

| Field | Value |
|---|---|
| Host | localhost |
| Port | 5432 |
| Database | citi-bike |
| User | `data_analyst` (read-only) or `data_engineer` |
| Password | `ANALYST_PASSWORD` or `DB_PASSWORD` from `.env` |

**ClickHouse (gold)**

| Field | Value |
|---|---|
| Host | localhost |
| Port | 9009 |
| Database | gold |
| User | `data_analyst` (read-only) or `data_engineer` |
| Password | `ANALYST_PASSWORD` or `CLICKHOUSE_ENGINEER_PASSWORD` from `.env` |

---

## Key Design Decisions

**Idempotency.** Every layer checks before writing. The pipeline is safe to re-run at any time and already-processed files are skipped at every stage.

**Medallion architecture.** Bronze is immutable. Silver is replayable from bronze. Gold is replayable from silver. A bug at any layer can be fixed and replayed without re-ingesting from source. Empty fields in source CSVs are interpreted as `NULL` at ingestion (`ingest.py`'s `pd.read_csv(..., keep_default_na=False, na_values=[""])`) — source files don't reliably distinguish empty strings from missing values, so no attempt is made to preserve that distinction, and only a truly empty field is coerced, not pandas' broader default list of "NA"/"NULL"/"N/A"-style tokens that could otherwise destroy a real value.

**SCD Type 2 on stations.** Station names and coordinates change over time. dbt snapshots track the full history, and `fact_trips` resolves each ride to the station version that was actually true at ride time via a ClickHouse `ASOF JOIN` on `valid_from`/`valid_to` — not just a lookup of whatever the station's attributes are today. `station_key` is unique per version (sourced from `dbt_scd_id`, not the natural `station_id`), which is what makes the point-in-time join actually mean something.

**ClickHouse bridge tables.** Gold models read from Postgres silver via ClickHouse's PostgreSQL engine. No data is copied and ClickHouse queries Postgres directly. Only the gold layer is physically stored in ClickHouse.

**Why bronze lives in Postgres, not queried straight off MinIO's Parquet.** ClickHouse can query S3-compatible object storage directly via its S3 table engine, so an obvious question is why this pipeline hops through a Postgres bronze table at all instead of pointing ClickHouse straight at the raw Parquet files. The answer is that Postgres gives the rest of the pipeline a mutable staging surface: dbt's incremental models need `MERGE`/upsert semantics keyed on `ride_id`, Soda's quality checks run as row-level SQL assertions against a real table, and re-loading a corrected file means updating rows in place rather than re-deriving the whole layer from immutable object storage. Object storage is the right fit for gold, where the shape is fixed and queries are append-mostly as it is the wrong fit for bronze, where the whole point is DML.

**Gold intentionally doesn't reconcile 1:1 with silver.** `fact_trips` excludes the small share of rides (~0.3%) whose start or end station can't be resolved to a `dim_station` row even after the name→id backfill (see [Silver](#silver)), so that `start_station_key`/`end_station_key` stay non-nullable and every FK in the fact table is guaranteed resolvable. The tradeoff: `fact_trips`'s row count will never exactly match `silver_trips`'s — a real, known gap, not a bug — and an analyst diffing total ride counts across layers should expect it. The alternative (a sentinel "Unknown" row in `dim_station` so every ride survives into the fact table) was considered and rejected: it would require every station-level query to remember to filter out a synthetic row that doesn't represent an unresolved-but-real station, just an absence of data.
