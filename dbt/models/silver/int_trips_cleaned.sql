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
        {{ convert_to_eastern('started_at') }} AS started_at,
        {{ convert_to_eastern('ended_at') }} AS ended_at,

        -- Derived time dimensions
        DATE({{ convert_to_eastern('started_at') }}) AS start_date,
        EXTRACT(HOUR FROM {{ convert_to_eastern('started_at') }})::INT AS start_hour,
        TO_CHAR({{ convert_to_eastern('started_at') }}, 'FMDay') AS day_of_week,

        -- Ride duration
        ROUND(EXTRACT(EPOCH FROM (ended_at - started_at)) / 60.0, 2) AS ride_duration_minutes,

        -- Station fields - nulls preserved
        {{ clean_string('start_station_name') }} AS start_station_name,
        {{ clean_string('start_station_id') }} AS start_station_id,
        {{ clean_string('end_station_name') }} AS end_station_name,
        {{ clean_string('end_station_id') }} AS end_station_id,

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

-- Station IDs are sometimes blank in a given month's source file even though
-- the same physical station (by name) has a real ID in other months — the ID
-- isn't expected to change, so a name with exactly one distinct known ID
-- elsewhere is safe to backfill. Names with zero or multiple candidate IDs
-- are left NULL rather than risk a wrong guess.
station_lookup AS (
    SELECT station_name, MAX(station_id) AS resolved_station_id
    FROM (
        SELECT start_station_name AS station_name, start_station_id AS station_id
        FROM cleaned
        WHERE start_station_id IS NOT NULL AND start_station_name IS NOT NULL

        UNION ALL

        SELECT end_station_name, end_station_id
        FROM cleaned
        WHERE end_station_id IS NOT NULL AND end_station_name IS NOT NULL

        {% if is_incremental() %}
        UNION ALL

        SELECT start_station_name, start_station_id
        FROM {{ this }}
        WHERE start_station_id IS NOT NULL AND start_station_name IS NOT NULL

        UNION ALL

        SELECT end_station_name, end_station_id
        FROM {{ this }}
        WHERE end_station_id IS NOT NULL AND end_station_name IS NOT NULL
        {% endif %}
    ) all_pairs
    GROUP BY station_name
    HAVING COUNT(DISTINCT station_id) = 1
),

backfilled AS (
    SELECT
        c.ride_id,
        c.rideable_type,
        c.started_at,
        c.ended_at,
        c.start_date,
        c.start_hour,
        c.day_of_week,
        c.ride_duration_minutes,
        c.start_station_name,
        COALESCE(c.start_station_id, sl_start.resolved_station_id) AS start_station_id,
        c.end_station_name,
        COALESCE(c.end_station_id, sl_end.resolved_station_id) AS end_station_id,
        c.start_lat,
        c.start_lng,
        c.end_lat,
        c.end_lng,
        c.member_casual,
        c._source_file,
        c._ingested_at
    FROM cleaned c
    LEFT JOIN station_lookup sl_start ON c.start_station_name = sl_start.station_name
    LEFT JOIN station_lookup sl_end ON c.end_station_name = sl_end.station_name
),

flagged AS (
    SELECT
        *,
        CASE
            WHEN ride_duration_minutes <= 0 THEN 'non_positive_duration'
            WHEN ride_duration_minutes >= 1440 THEN 'duration_exceeds_24h'
            WHEN start_lat IS NULL THEN 'missing_start_lat'
            WHEN start_lng IS NULL THEN 'missing_start_lng'
            ELSE NULL
        END AS rejection_reason
    FROM backfilled
)

SELECT * FROM flagged
