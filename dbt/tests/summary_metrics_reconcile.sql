-- Fail when serving marts lose additive information needed for correct means.
WITH expected AS (
    SELECT count() AS rides,
           sum(ride_duration_minutes) AS duration_sum,
           count(ride_duration_minutes) AS duration_count,
           sum(ride_distance_km) AS distance_sum,
           count(ride_distance_km) AS distance_count,
           sum(temperature_c) AS temp_sum,
           count(temperature_c) AS temp_count
    FROM {{ ref('fact_trips') }}
), daily AS (
    SELECT sum(total_rides) AS rides,
           sum(ride_duration_sum) AS duration_sum,
           sum(ride_duration_count) AS duration_count,
           sum(ride_distance_sum) AS distance_sum,
           sum(ride_distance_count) AS distance_count,
           sum(temperature_sum) AS temp_sum,
           sum(temperature_count) AS temp_count
    FROM {{ ref('daily_ride_summary') }}
), city AS (
    SELECT sum(rides) AS rides,
           sum(ride_duration_sum) AS duration_sum,
           sum(ride_duration_count) AS duration_count,
           sum(ride_distance_sum) AS distance_sum,
           sum(ride_distance_count) AS distance_count
    FROM {{ ref('mart_city_flow') }}
)
SELECT 'daily_ride_summary' AS failed_model
FROM expected e CROSS JOIN daily d
WHERE e.rides != d.rides OR e.duration_count != d.duration_count
   OR e.distance_count != d.distance_count OR e.temp_count != d.temp_count
   OR abs(e.duration_sum - d.duration_sum) > greatest(0.000001, abs(e.duration_sum) * 0.000000001)
   OR abs(e.distance_sum - d.distance_sum) > greatest(0.000001, abs(e.distance_sum) * 0.000000001)
   OR abs(e.temp_sum - d.temp_sum) > greatest(0.000001, abs(e.temp_sum) * 0.000000001)
UNION ALL
SELECT 'mart_city_flow' AS failed_model
FROM expected e CROSS JOIN city c
WHERE e.rides != c.rides OR e.duration_count != c.duration_count
   OR e.distance_count != c.distance_count
   OR abs(e.duration_sum - c.duration_sum) > greatest(0.000001, abs(e.duration_sum) * 0.000000001)
   OR abs(e.distance_sum - c.distance_sum) > greatest(0.000001, abs(e.distance_sum) * 0.000000001)
