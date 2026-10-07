{{
    config(
        materialized='table',
        schema='gold',
        engine='MergeTree()',
        order_by='full_date'
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="cast('2021-01-01' as date)",
        end_date="cast(now() as date)"
    ) }}
),

max_date AS (
    -- Latest of start and end dates: a ride can end on a later calendar day
    -- than any ride starts, and arrival-side marts join dim_date on that day.
    SELECT MAX(greatest(start_date, toDate(ended_at))) AS max_trip_date
    FROM {{ source('silver', 'silver_trips') }}
),

final AS (
    SELECT
        toYYYYMMDD(date_day) AS date_key,
        date_day AS full_date,
        toYear(date_day) AS year,
        toMonth(date_day) AS month,
        toDayOfMonth(date_day) AS day,
        toQuarter(date_day) AS quarter,
        toISOWeek(date_day) AS week_of_year,
        toDayOfWeek(date_day) AS day_of_week_num,
        {{ day_name('date_day') }} AS day_name,
        {{ month_name('date_day') }} AS month_name,
        if(toDayOfWeek(date_day) IN (6, 7), 1, 0) AS is_weekend,
        {{ get_season('date_day') }} AS season,
        formatDateTime(date_day, '%b %Y') AS month_year,
        toYYYYMM(date_day) AS month_year_key
    FROM date_spine
    CROSS JOIN max_date
    WHERE date_day <= max_trip_date
)

SELECT * FROM final
