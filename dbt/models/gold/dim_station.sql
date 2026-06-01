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
        station_key,
        station_id,
        station_name,
        latitude,
        longitude,
        dbt_valid_from,
        dbt_valid_to,
        {{ is_current_flag('dbt_valid_to') }}   AS is_current,
        dbt_scd_id
    FROM {{ source('snapshots', 'station_snapshot') }}
)

SELECT 
    assumeNotNull(station_key)      AS station_key,
    station_id,
    station_name,
    latitude,
    longitude,
    dbt_valid_from                  AS valid_from,
    dbt_valid_to                    AS valid_to,
    is_current,
    dbt_scd_id
FROM snapshot