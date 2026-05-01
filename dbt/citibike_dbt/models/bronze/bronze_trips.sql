{{
    config(
        materialized='table',
        schema='bronze'
    )
}}

SELECT
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    _source_file,
    _ingested_at
FROM {{ source('bronze', 'trips') }}