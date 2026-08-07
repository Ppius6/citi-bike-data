{{
    config(
        materialized='table',
        schema='gold',
        engine='MergeTree()',
        order_by='weather_code'
    )
}}

-- WMO weather interpretation codes, as returned by Open-Meteo's `weathercode`
-- field (https://open-meteo.com/en/docs#weathervariables). A fixed reference
-- set, not derived from observed data — fact_trips.weather_code joins to
-- this table's weather_code directly (no surrogate key needed).
SELECT
    weather_code,
    weather_description
FROM VALUES(
    'weather_code UInt8, weather_description String',
    -- Clear sky & clouds
    (0, 'Clear sky'),
    (1, 'Mainly clear'),
    (2, 'Partly cloudy'),
    (3, 'Overcast'),
    -- Fog & dust
    (45, 'Fog'),
    (48, 'Depositing rime fog'),
    -- Drizzle
    (51, 'Light drizzle'),
    (53, 'Moderate drizzle'),
    (55, 'Dense drizzle'),
    (56, 'Light freezing drizzle'),
    (57, 'Dense freezing drizzle'),
    -- Rain
    (61, 'Slight rain'),
    (63, 'Moderate rain'),
    (65, 'Heavy rain'),
    (66, 'Light freezing rain'),
    (67, 'Heavy freezing rain'),
    -- Snow
    (71, 'Slight snowfall'),
    (73, 'Moderate snowfall'),
    (75, 'Heavy snowfall'),
    (77, 'Snow grains'),
    -- Showers
    (80, 'Slight rain showers'),
    (81, 'Moderate rain showers'),
    (82, 'Violent rain showers'),
    (85, 'Slight snow showers'),
    (86, 'Heavy snow showers'),
    -- Thunderstorms
    (95, 'Slight or moderate thunderstorm'),
    (96, 'Thunderstorm with slight hail'),
    (99, 'Thunderstorm with heavy hail')
)
