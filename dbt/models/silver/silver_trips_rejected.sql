{{
    config(
        materialized='view',
        schema='silver'
    )
}}

SELECT
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_date,
    start_hour,
    day_of_week,
    ride_duration_minutes,
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
    _ingested_at,
    rejection_reason
FROM {{ ref('int_trips_cleaned') }}
WHERE rejection_reason IS NOT NULL
