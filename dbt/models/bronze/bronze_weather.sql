{{
    config(
        materialized='view'
    )
}}

SELECT
    observation_time,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    weather_code,
    is_daylight,
    _ingested_at
FROM {{ source('bronze', 'weather') }}
