#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS snapshots;

    CREATE USER data_engineer WITH PASSWORD '${DB_PASSWORD}';
    CREATE USER data_analyst WITH PASSWORD '${ANALYST_PASSWORD}';

    GRANT CREATE ON DATABASE "${POSTGRES_DB}" TO data_engineer;
    GRANT USAGE, CREATE ON SCHEMA bronze TO data_engineer;
    GRANT USAGE, CREATE ON SCHEMA silver TO data_engineer;
    GRANT USAGE, CREATE ON SCHEMA snapshots TO data_engineer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO data_engineer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO data_engineer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA snapshots GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO data_engineer;

    -- Created after the default-privileges grants above so data_engineer
    -- (the loader's COPY target) picks up SELECT/INSERT/UPDATE/DELETE
    -- automatically, the same way any later dbt-created table would.
    CREATE TABLE IF NOT EXISTS bronze.trips (
        ride_id text,
        rideable_type text,
        started_at timestamptz,
        ended_at timestamptz,
        start_station_name text,
        start_station_id text,
        end_station_name text,
        end_station_id text,
        start_lat double precision,
        start_lng double precision,
        end_lat double precision,
        end_lng double precision,
        member_casual text,
        _source_file text,
        _ingested_at timestamptz
    );

    GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO data_engineer;
    GRANT SELECT ON ALL TABLES IN SCHEMA silver TO data_engineer;
    GRANT SELECT ON ALL TABLES IN SCHEMA snapshots TO data_engineer;

    GRANT USAGE ON SCHEMA silver TO data_analyst;
    ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO data_analyst;
EOSQL