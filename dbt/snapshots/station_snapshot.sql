{% snapshot station_snapshot %}
    {{
        config(
            target_schema='snapshots',
            unique_key='station_id',
            strategy='check',
            check_cols=['station_name'],
            invalidate_hard_deletes=True
        )
    }}
    
WITH start_stations AS (
    SELECT
        start_station_id AS station_id,
        start_station_name AS station_name,
        ROUND(AVG(start_lat)::NUMERIC, 6) AS latitude,
        ROUND(AVG(start_lng)::NUMERIC, 6) AS longitude
    FROM {{ ref('silver_trips') }}
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id, start_station_name
),

end_stations AS (
    SELECT
        end_station_id AS station_id,
        end_station_name AS station_name,
        ROUND(AVG(end_lat)::NUMERIC, 6) AS latitude,
        ROUND(AVG(end_lng)::NUMERIC, 6) AS longitude
    FROM {{ ref('silver_trips') }}
    WHERE end_station_id IS NOT NULL
    GROUP BY end_station_id, end_station_name
),

all_stations AS (
    SELECT * FROM start_stations
    UNION ALL
    SELECT * FROM end_stations
),

deduped AS (
    SELECT DISTINCT ON (station_id)
        station_id,
        station_name,
        latitude,
        longitude
    FROM all_stations
    ORDER BY station_id, station_name
)

SELECT * FROM deduped

{% endsnapshot %}