{{
    config(
        materialized='table',
        schema='marts',
        engine='MergeTree()',
        order_by='(month_start, start_city, end_city)',
    )
}}

-- Rides between cities per month: where rides start and where they end, so
-- Jersey City <-> Hoboken crossings are one row each way. City comes from
-- dim_station (derived from the station ID format).

WITH trips AS (
    SELECT
        start_station_key,
        end_station_key,
        started_at,
        ride_duration_minutes,
        ride_distance_km
    FROM {{ ref('fact_trips') }}
),

stations AS (
    SELECT station_key, city FROM {{ ref('dim_station') }}
),

joined AS (
    SELECT
        toStartOfMonth(t.started_at)    AS month_start,
        s.city                          AS start_city,
        e.city                          AS end_city,
        t.ride_duration_minutes         AS ride_duration_minutes,
        t.ride_distance_km              AS ride_distance_km
    FROM trips t
    LEFT JOIN stations s ON t.start_station_key = s.station_key
    LEFT JOIN stations e ON t.end_station_key = e.station_key
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['month_start', 'start_city', 'end_city']) }} AS city_flow_key,
    month_start,
    start_city,
    end_city,
    count()                                    AS rides,
    round(avg(ride_duration_minutes), 2)       AS avg_ride_duration_minutes,
    round(avg(ride_distance_km), 2)            AS avg_ride_distance_km
FROM joined
GROUP BY month_start, start_city, end_city
