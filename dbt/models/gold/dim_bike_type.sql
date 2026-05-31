{{
    config(
        materialized='table',
        schema='gold',
        engine='MergeTree()',
        order_by='bike_type_key'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['rideable_type']) }} AS bike_type_key,
    rideable_type AS bike_type,
    multiIf(
        rideable_type = 'classic_bike', 'Classic Bike',
        rideable_type = 'electric_bike', 'Electric Bike',
        rideable_type = 'docked_bike', 'Docked Bike',
        'Unknown'
    ) AS bike_type_desc
FROM (
    SELECT DISTINCT rideable_type
    FROM {{ source('silver', 'silver_trips') }}
    WHERE rideable_type IS NOT NULL
)