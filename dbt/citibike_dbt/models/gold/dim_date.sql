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
    SELECT MAX(start_date) AS max_start_date
    FROM {{ source('silver', 'silver_trips') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
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
        {{ get_season('date_day') }} AS season
    FROM date_spine
    CROSS JOIN max_date
    WHERE date_day <= max_start_date
)

SELECT * FROM final
