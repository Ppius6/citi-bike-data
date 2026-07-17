{{
    config(
        materialized='incremental',
        schema='silver',
        unique_key='observation_time',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('bronze_weather') }}

    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1970-01-01'::timestamp) FROM {{ this }})
    {% endif %}
),

converted AS (
    SELECT
        -- Fact trips started_at is converted to Eastern, so let's match it exactly.
        {{ convert_to_eastern('observation_time') }} AS observation_time,
        
        COALESCE(temperature_c, 0.0) AS temperature_c,
        COALESCE(precipitation_mm, 0.0) AS precipitation_mm,
        COALESCE(wind_speed_kmh, 0.0) AS wind_speed_kmh,
        COALESCE(weather_code, 0) AS weather_code,
        COALESCE(is_daylight, false) AS is_daylight,
        
        _ingested_at
    FROM source
),

deduplicated AS (
    SELECT * from (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY observation_time ORDER BY _ingested_at DESC
        ) AS rn
    FROM converted
    ) ranked WHERE rn = 1
)

SELECT
    observation_time,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    weather_code,
    is_daylight,
    _ingested_at
FROM deduplicated
