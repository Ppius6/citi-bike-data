-- Reconciliation check for the silver -> gold hop: fact_trips intentionally
-- drops silver_trips rows with no resolvable start/end station (see the
-- `trips` CTE in fact_trips.sql). This asserts that gap is *exactly*
-- accounted for by that one known cause — any other divergence between the
-- two row counts fails the test instead of passing silently.

WITH silver AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN start_station_id IS NULL OR end_station_id IS NULL THEN 1 ELSE 0 END) AS expected_dropped_rows
    FROM {{ source('silver', 'silver_trips') }}
),

gold AS (
    SELECT COUNT(*) AS total_rows
    FROM {{ ref('fact_trips') }}
)

SELECT
    silver.total_rows AS silver_trips_count,
    gold.total_rows AS fact_trips_count,
    silver.expected_dropped_rows AS expected_dropped_count,
    silver.total_rows - gold.total_rows AS actual_dropped_count
FROM silver
CROSS JOIN gold
WHERE silver.total_rows - gold.total_rows != silver.expected_dropped_rows