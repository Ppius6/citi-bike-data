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
        multiIf(
            toDayOfWeek(date_day) = 1, 'Monday',
            toDayOfWeek(date_day) = 2, 'Tuesday',
            toDayOfWeek(date_day) = 3, 'Wednesday',
            toDayOfWeek(date_day) = 4, 'Thursday',
            toDayOfWeek(date_day) = 5, 'Friday',
            toDayOfWeek(date_day) = 6, 'Saturday',
            'Sunday'
        )                                                       AS day_name,
        multiIf(
            toMonth(date_day) = 1,  'January',
            toMonth(date_day) = 2,  'February',
            toMonth(date_day) = 3,  'March',
            toMonth(date_day) = 4,  'April',
            toMonth(date_day) = 5,  'May',
            toMonth(date_day) = 6,  'June',
            toMonth(date_day) = 7,  'July',
            toMonth(date_day) = 8,  'August',
            toMonth(date_day) = 9,  'September',
            toMonth(date_day) = 10, 'October',
            toMonth(date_day) = 11, 'November',
            'December'
        ) AS month_name,
        if(toDayOfWeek(date_day) IN (6, 7), 1, 0) AS is_weekend,
        multiIf(
             toMonth(date_day) IN (12, 1, 2), 'Winter',
             toMonth(date_day) IN (3, 4, 5), 'Spring',
             toMonth(date_day) IN (6, 7, 8), 'Summer',
             'Fall'
        ) AS season
    FROM date_spine
    CROSS JOIN max_date
    WHERE date_day <= max_start_date
)

SELECT * FROM final
