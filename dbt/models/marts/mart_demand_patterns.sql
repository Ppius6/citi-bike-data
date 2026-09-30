{{
    config(
        materialized='table',
        schema='marts',
        engine='MergeTree()',
        order_by='(station_id, day_of_week, hour)',
    )
}}

with flow as (
    select
        ds.station_id                AS station_id,
        dd.full_date                 AS full_date,
        sf.hour                      AS hour,
        sum(sf.departures)           AS departures,
        sum(sf.arrivals)             AS arrivals
    from {{ ref('mart_station_flow') }} sf
    join {{ ref('dim_station') }} ds ON sf.station_key = ds.station_key
    join {{ ref('dim_date') }} dd ON sf.date_key = dd.date_key
    group by station_id, full_date, hour
),

station_span as (
    select
        station_id,
        min(full_date) AS first_date,
        max(full_date) AS last_date
    from flow
    group by station_id
),

grid as (
    select
        s.station_id as station_id,
        d.full_date as full_date,
        h.hour as hour
    from station_span s
    cross join (select full_date from {{ ref('dim_date') }}) d
    cross join (select toUInt8(number) as hour from numbers(24)) h
    where d.full_date between s.first_date and s.last_date 
),

dense as (
    select
        g.station_id                         AS station_id,
        toDayOfWeek(g.full_date)             AS day_of_week,
        g.hour                               AS hour,
        coalesce(f.departures, 0)            AS departures,
        coalesce(f.arrivals, 0)              AS arrivals
    from grid g
    left join flow f
        on g.station_id = f.station_id
        and g.full_date = f.full_date
        and g.hour = f.hour
)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'day_of_week', 'hour']) }} AS demand_pattern_key,
    station_id,
    day_of_week,
    hour,
    count()                                        AS sample_size,
    round(avg(departures), 2)                      AS avg_departures,
    quantile(0.05)(departures)                     AS p05_departures,
    quantile(0.95)(departures)                     AS p95_departures,
    round(avg(arrivals), 2)                        AS avg_arrivals,
    quantile(0.05)(arrivals)                       AS p05_arrivals,
    quantile(0.95)(arrivals)                       AS p95_arrivals,
    round(avg(departures - arrivals), 2)           AS avg_net_flow
from dense
group by 
    station_id, day_of_week, hour