#!/bin/bash
set -e

clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS data_engineer IDENTIFIED BY 'dataengineer7yeh'"
clickhouse-client --port 9009 --query "CREATE USER IF NOT EXISTS data_analyst IDENTIFIED BY 'analyst839#'"

clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS gold"
clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS silver"
clickhouse-client --port 9009 --query "CREATE DATABASE IF NOT EXISTS snapshots"

clickhouse-client --port 9009 --query "GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, DROP TABLE ON gold.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE ON silver.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE ON snapshots.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT, SHOW ON system.* TO data_engineer"
clickhouse-client --port 9009 --query "GRANT SELECT ON gold.* TO data_analyst"
clickhouse-client --port 9009 --query "GRANT SELECT ON silver.* TO data_analyst"

echo "ClickHouse init complete."