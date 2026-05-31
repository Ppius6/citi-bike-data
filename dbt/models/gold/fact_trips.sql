{{
    config(
        materialized='table',
        schema='gold',
        engine='ReplacingMergeTree()',
        order_by='trip_key',
    )
}}

WITH trips AS (
    SELECT
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_date,
        start_hour,
        day_of_week,
        ride_duration_minutes,
        start_station_id,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        _source_file,
        _ingested_at
    FROM {{ source('silver', 'silver_trips') }}
),

dim_date AS (
    SELECT 
        date_key, 
        full_date
    FROM {{ ref('dim_date') }}
),

dim_station_start AS (
    SELECT
        station_key,
        station_id
    FROM {{ ref('dim_station') }}
    WHERE is_current = 1
),

dim_station_end AS (
    SELECT
        station_key,
        station_id
    FROM {{ ref('dim_station') }}
    WHERE is_current = 1
),

dim_rider AS (
    SELECT
        rider_type_key, 
        rider_type
    FROM {{ ref('dim_rider_type') }}
),

dim_bike AS (
    SELECT
        bike_type_key, 
        bike_type
    FROM {{ ref('dim_bike_type') }}
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['t.ride_id']) }}   AS trip_key,

        -- Natural key
        t.ride_id,

        -- Foreign keys
        dd.date_key,
        ds.station_key                                          AS start_station_key,
        de.station_key                                          AS end_station_key,
        dr.rider_type_key,
        db.bike_type_key,

        -- Timestamps
        t.started_at,
        t.ended_at,

        -- Measures
        t.ride_duration_minutes,
        t.start_hour,
        t.day_of_week,

        -- Coordinates
        t.start_lat,
        t.start_lng,
        t.end_lat,
        t.end_lng,

        -- Metadata
        t._source_file,
        t._ingested_at

    FROM trips t
    LEFT JOIN dim_date dd
        ON t.start_date = dd.full_date
    LEFT JOIN dim_station_start ds
        ON t.start_station_id = ds.station_id
    LEFT JOIN dim_station_end de
        ON t.end_station_id = de.station_id
    LEFT JOIN dim_rider dr
        ON t.member_casual = dr.rider_type
    LEFT JOIN dim_bike db
        ON t.rideable_type = db.bike_type
)

SELECT * FROM final