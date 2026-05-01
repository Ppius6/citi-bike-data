{{
    config(
        materialized='incremental',
        schema='silver',
        unique_key='ride_id',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('bronze_trips') }}

    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT * from (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ride_id ORDER BY _ingested_at DESC
        ) AS rn
    FROM source
    ) ranked WHERE rn = 1
),

cleaned AS (
    SELECT 
        ride_id,
        LOWER(TRIM(rideable_type)) AS rideable_type,

        -- Timezone conversion from UTC to New York
        started_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS started_at,
        ended_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS ended_at,

        -- Derived time dimensions
        DATE(started_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS start_date,
        EXTRACT(HOUR FROM started_at AT TIME ZONE 'America/New_York')::INT AS start_hour,
        TO_CHAR(started_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York', 'Day') AS day_of_week,

        -- Ride duration
        ROUND(EXTRACT(EPOCH FROM (ended_at - started_at)) / 60.0, 2) AS ride_duration_minutes,

        -- Station fields - nulls preserved 
        NULLIF(TRIM(start_station_name), '') AS start_station_name,
        NULLIF(TRIM(start_station_id), '') AS start_station_id,
        NULLIF(TRIM(end_station_name), '') AS end_station_name,
        NULLIF(TRIM(end_station_id), '') AS end_station_id,

        -- Coordinates
        start_lat,
        start_lng,
        end_lat,
        end_lng,

        -- Rider type
        LOWER(TRIM(member_casual)) AS member_casual,

        -- Metadata
        _source_file,
        _ingested_at

    FROM deduplicated
),

validated AS (
    SELECT *
    FROM cleaned
    WHERE 
        -- Remove rides with invalid durations
        ride_duration_minutes > 0
        AND ride_duration_minutes < 1440 -- 24 hours max

        -- Remove rides missing coordinates
        AND start_lat IS NOT NULL
        AND start_lng IS NOT NULL
)

SELECT * FROM validated