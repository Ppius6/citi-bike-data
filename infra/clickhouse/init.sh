#!/bin/bash
set -e

clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS data_engineer IDENTIFIED BY '${CLICKHOUSE_ENGINEER_PASSWORD}'"
clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS data_analyst IDENTIFIED BY '${ANALYST_PASSWORD}'"
clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS ai_agent IDENTIFIED BY '${AI_AGENT_PASSWORD}'"

clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS gold"
clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS silver"
clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS snapshots"
clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS agent"

# Agent Memory Table
clickhouse-client --port 9009 --query "
CREATE TABLE IF NOT EXISTS agent.memory
(
    id UUID DEFAULT generateUUIDv4(),
    question String,
    query String,
    embedding Array(Float32),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY id
"

# Bridge tables — ClickHouse's PostgreSQL engine reads these live from Postgres,
# no data copied. dbt's gold models reference them via source(), which assumes
# they already exist; dbt itself never creates them.
clickhouse-client --port 9009 --query "
CREATE TABLE IF NOT EXISTS silver.silver_trips
(
    ride_id String,
    rideable_type String,
    started_at DateTime64(6),
    ended_at DateTime64(6),
    start_date Date,
    start_hour Int32,
    day_of_week String,
    ride_duration_minutes Float64,
    start_station_name Nullable(String),
    start_station_id Nullable(String),
    end_station_name Nullable(String),
    end_station_id Nullable(String),
    start_lat Float64,
    start_lng Float64,
    end_lat Nullable(Float64),
    end_lng Nullable(Float64),
    member_casual String,
    _source_file String,
    _ingested_at DateTime64(6)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'silver_trips', 'data_engineer', '${DB_PASSWORD}', 'silver')
"

clickhouse-client --port 9009 --query "
CREATE TABLE IF NOT EXISTS snapshots.station_snapshot
(
    station_id String,
    station_name String,
    latitude Float64,
    longitude Float64,
    dbt_scd_id String,
    dbt_updated_at DateTime64(6),
    dbt_valid_from DateTime64(6),
    dbt_valid_to Nullable(DateTime64(6))
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'station_snapshot', 'data_engineer', '${DB_PASSWORD}', 'snapshots')
"

clickhouse-client --port 9009 --query "GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, DROP TABLE ON gold.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE ON silver.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE ON snapshots.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT, SHOW ON system.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT ON gold.* TO data_analyst"
clickhouse-client --port 9009 --query "GRANT SELECT ON silver.* TO data_analyst"

# AI agent — scoped to the gold layer only, plus system.columns/tables so it can introspect the schema
clickhouse-client --port 9009 --query "GRANT SELECT ON gold.* TO ai_agent"
clickhouse-client --port 9009 --query "GRANT SELECT ON system.columns TO ai_agent"
clickhouse-client --port 9009 --query "GRANT SELECT ON system.tables TO ai_agent"
clickhouse-client --port 9009 --query "GRANT SELECT, INSERT ON agent.memory TO ai_agent"

echo "ClickHouse init complete."