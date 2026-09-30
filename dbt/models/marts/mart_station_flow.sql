{{
    config(
        materialized='table',
        schema='marts',
        engine='MergeTree()',
        order_by='(date_key, station_key, hour)',
    )
}}

-- Station-level operational flow: departures and arrivals per station per
-- hour, attributed to the hour the ride actually started/ended (not the
-- same hour for both sides of a long-running ride). Each trip contributes
-- one departure-tagged row and one arrival-tagged row to a single unioned
-- stream, aggregated once with conditional counts — a station-hour that
-- only saw one side of traffic naturally gets 0 on the other side from
-- countIf, no separate spine/outer-join needed.

WITH trips AS (
    SELECT
        start_station_key,
        end_station_key,
        date_key,
        started_at,
        ended_at,
        rider_type_key,
        bike_type_key,
        ride_distance_km
    FROM {{ ref('fact_trips') }}
),

dim_date AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

dim_rider AS (
    SELECT rider_type_key, rider_type FROM {{ ref('dim_rider_type') }}
),

dim_bike AS (
    SELECT bike_type_key, bike_type FROM {{ ref('dim_bike_type') }}
),

events AS (
    SELECT
        t.start_station_key                          AS station_key,
        t.date_key                                    AS date_key,
        toHour(t.started_at)                          AS hour,
        'departure'                                   AS event_type,
        dr.rider_type                                 AS rider_type,
        db.bike_type                                  AS bike_type,
        t.ride_distance_km                            AS ride_distance_km,
        (t.start_station_key = t.end_station_key)     AS is_round_trip
    FROM trips t
    LEFT JOIN dim_rider dr ON t.rider_type_key = dr.rider_type_key
    LEFT JOIN dim_bike db ON t.bike_type_key = db.bike_type_key

    UNION ALL

    -- Arrivals get their own date_key from ended_at, distinct from
    -- trips.date_key (keyed off the ride's start_date) — a ride can end on
    -- a different calendar day than it started. ride_distance_km/round-trip
    -- are departure-side concepts, so they're null/false here and excluded
    -- from every arrival-side aggregate below.
    SELECT
        t.end_station_key                             AS station_key,
        ad.date_key                                   AS date_key,
        toHour(t.ended_at)                             AS hour,
        'arrival'                                      AS event_type,
        dr.rider_type                                 AS rider_type,
        db.bike_type                                  AS bike_type,
        NULL                                            AS ride_distance_km,
        false                                           AS is_round_trip
    FROM trips t
    LEFT JOIN dim_date ad ON toDate(t.ended_at) = ad.full_date
    LEFT JOIN dim_rider dr ON t.rider_type_key = dr.rider_type_key
    LEFT JOIN dim_bike db ON t.bike_type_key = db.bike_type_key
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['station_key', 'date_key', 'hour']) }} AS station_flow_key,
        station_key,
        date_key,
        hour,

        countIf(event_type = 'departure')                                          AS departures,
        countIf(event_type = 'arrival')                                            AS arrivals,
        countIf(event_type = 'departure') - countIf(event_type = 'arrival')        AS net_flow,

        countIf(event_type = 'departure' AND rider_type = 'member')                AS member_departures,
        countIf(event_type = 'departure' AND rider_type = 'casual')                AS casual_departures,
        countIf(event_type = 'departure' AND bike_type = 'electric_bike')          AS electric_departures,
        countIf(event_type = 'departure' AND bike_type = 'classic_bike')           AS classic_departures,
        countIf(event_type = 'departure' AND bike_type = 'docked_bike')            AS docked_departures,

        countIf(event_type = 'arrival' AND rider_type = 'member')                  AS member_arrivals,
        countIf(event_type = 'arrival' AND rider_type = 'casual')                  AS casual_arrivals,
        countIf(event_type = 'arrival' AND bike_type = 'electric_bike')            AS electric_arrivals,
        countIf(event_type = 'arrival' AND bike_type = 'classic_bike')             AS classic_arrivals,
        countIf(event_type = 'arrival' AND bike_type = 'docked_bike')              AS docked_arrivals,

        countIf(event_type = 'departure' AND is_round_trip)                        AS round_trip_count,
        -- Null (not 0) when there were no departures at all — an undefined
        -- share is not the same as a station with a genuine 0% round-trip rate.
        CASE WHEN countIf(event_type = 'departure') = 0 THEN NULL
             ELSE round(countIf(event_type = 'departure' AND is_round_trip) / countIf(event_type = 'departure'), 4)
        END                                                                          AS round_trip_share,

        round(sumIf(ride_distance_km, event_type = 'departure'), 2)                AS distance_weighted_outflow

    FROM events
    GROUP BY station_key, date_key, hour
)

SELECT * FROM final
