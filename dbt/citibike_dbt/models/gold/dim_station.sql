{{
    config(
        materialized='table',
        schema='gold',
        engine='MergeTree()',
        order_by='station_id'
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
        CASE 
            WHEN dbt_valid_to IS NULL THEN 1 ELSE 0
        END AS is_current,
        dbt_scd_id
    FROM {{ source('snapshots', 'station_snapshot') }}
)

SELECT 
    station_key,
    station_id,
    station_name,
    latitude,
    longitude,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    is_current,
    dbt_scd_id
FROM snapshot