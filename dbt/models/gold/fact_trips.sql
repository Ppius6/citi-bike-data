{{
    config(
        materialized='table',
        schema='gold',
        engine='MergeTree()',
        order_by='(date_key, start_station_key)',
    )
}}

WITH trips AS (
    SELECT
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_date,
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
    -- A small share of rides (~0.3%) have no station recorded/recoverable
    -- (see silver's station_lookup backfill) — excluded here so every fact
    -- row's station FKs are fully resolved, keeping them non-nullable.
    WHERE start_station_id IS NOT NULL
      AND end_station_id IS NOT NULL
),

dim_date AS (
    SELECT
        date_key,
        full_date
    FROM {{ ref('dim_date') }}
),

dim_station AS (
    SELECT
        station_key,
        station_id,
        valid_from
    FROM {{ ref('dim_station') }}
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

weather AS (
    SELECT
        observation_time,
        temperature_c,
        precipitation_mm,
        wind_speed_kmh,
        weather_code,
        is_daylight
    FROM {{ source('silver', 'silver_weather') }}
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
        -- Great-circle distance; NULL when the end point is unknown, which
        -- is correct — an unknown endpoint means an unknown distance.
        round(geoDistance(t.start_lng, t.start_lat, t.end_lng, t.end_lat) / 1000, 2) AS ride_distance_km,

        -- Coordinates
        t.start_lat,
        t.start_lng,
        t.end_lat,
        t.end_lng,

        -- Weather
        w.temperature_c,
        w.precipitation_mm,
        w.wind_speed_kmh,
        w.weather_code,
        w.is_daylight,

        -- Metadata
        t._source_file,
        t._ingested_at

    FROM trips t
    LEFT JOIN dim_date dd
        ON t.start_date = dd.full_date
    LEFT ASOF JOIN dim_station ds
        ON t.start_station_id = ds.station_id
        AND t.started_at >= ds.valid_from
    LEFT ASOF JOIN dim_station de
        ON t.end_station_id = de.station_id
        AND t.started_at >= de.valid_from
    LEFT JOIN dim_rider dr
        ON t.member_casual = dr.rider_type
    LEFT JOIN dim_bike db
        ON t.rideable_type = db.bike_type
    LEFT JOIN weather w
        ON toStartOfHour(t.started_at) = w.observation_time
)

SELECT * FROM final