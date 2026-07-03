{{
    config(
        materialized='table',
        schema='gold',
        engine='MergeTree()',
        order_by='station_key'
    )
}}

WITH snapshot AS (
    SELECT
        dbt_scd_id,
        station_id,
        station_name,
        latitude,
        longitude,
        dbt_valid_from,
        dbt_valid_to,
        {{ is_current_flag('dbt_valid_to') }}   AS is_current
    FROM {{ source('snapshots', 'station_snapshot') }}
)

SELECT
    -- Surrogate key: one row per SCD2 version, not per station_id.
    -- station_id alone is NOT unique here (a station has one row per version).
    assumeNotNull(dbt_scd_id)       AS station_key,
    station_id,
    station_name,
    latitude,
    longitude,
    -- A station's earliest captured version reflects "however far back our
    -- data goes", not literally the moment the snapshot pipeline first ran —
    -- so it's backdated to an epoch sentinel rather than its true capture
    -- time. Only later, genuinely-detected changes keep a real valid_from.
    if(
        dbt_valid_from = min(dbt_valid_from) OVER (PARTITION BY station_id),
        toDateTime64('1970-01-01 00:00:00', 6),
        dbt_valid_from
    )                                AS valid_from,
    dbt_valid_to                    AS valid_to,
    is_current
FROM snapshot