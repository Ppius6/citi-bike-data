{{
    config(
        materialized='materialized_view',
        schema='gold',
        engine='MergeTree()',
        order_by='(date_key, rider_type_key, bike_type_key, weather_code)',
        refreshable={
            'interval': 'EVERY 1 DAY'
        }
    )
}}

-- Serving layer: pre-aggregates fact_trips down to one row per
-- (date, rider type, bike type, weather code, daylight) combination — the
-- exact grain most "how many rides under condition X" questions need,
-- without scanning the full fact table. A Refreshable MV (not a classic
-- trigger-based one) because fact_trips is rebuilt via a full table swap
-- each pipeline run, not row-by-row INSERTs, so nothing would ever trigger
-- a classic MV. The 1-day schedule is a backstop; the pipeline explicitly
-- forces a refresh via SYSTEM REFRESH VIEW right after each gold rebuild
-- (see scripts/orchestration/flows.py's refresh_gold_serving_views_task).
SELECT
    date_key,
    rider_type_key,
    bike_type_key,
    weather_code,
    is_daylight,
    count() AS total_rides,
    round(avg(ride_duration_minutes), 2) AS avg_ride_duration_minutes,
    round(avg(ride_distance_km), 2) AS avg_ride_distance_km,
    round(avg(temperature_c), 2) AS avg_temperature_c
FROM {{ ref('fact_trips') }}
GROUP BY date_key, rider_type_key, bike_type_key, weather_code, is_daylight
