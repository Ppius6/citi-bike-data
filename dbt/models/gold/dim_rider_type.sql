{{
    config(
        materialized='table',
        schema='gold',
        engine='MergeTree()',
        order_by='rider_type_key'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['member_casual']) }} AS rider_type_key,
    member_casual AS rider_type,
    multiIf(
        member_casual = 'member', 'Member',
        member_casual = 'casual', 'Casual',
        'Unknown'
    ) AS rider_type_desc
FROM (
    SELECT DISTINCT member_casual
    FROM {{ source('silver', 'silver_trips') }}
    WHERE member_casual IS NOT NULL
)