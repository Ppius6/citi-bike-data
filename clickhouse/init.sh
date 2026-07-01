#!/bin/bash
set -e

clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS data_engineer IDENTIFIED BY '${CLICKHOUSE_ENGINEER_PASSWORD}'"
clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS data_analyst IDENTIFIED BY '${ANALYST_PASSWORD}'"
clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS ai_agent IDENTIFIED BY '${AI_AGENT_PASSWORD}' SETTINGS readonly = 2"

clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS gold"
clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS silver"
clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS snapshots"

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

echo "ClickHouse init complete."