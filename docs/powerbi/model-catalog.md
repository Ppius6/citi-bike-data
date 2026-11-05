# Citi Bike Analysis — Definitions & Calculations Catalog

Catalog of every table, calculated object, relationship and measure in the **Citi Bike Analysis** Power BI semantic model, taken from the open Power BI Desktop file on 2026-11-05.

| | |
|---|---|
| **Model contents** | 16 tables (10 from ClickHouse, 6 helper tables), 52 core measures documented here, 15 relationships |
| **Source** | ClickHouse, schemas `gold` and `marts`. All tables in Import mode |
| **Data in the model** | 5,294,886 rides, 2021-01-31 to 2026-09-30, 470 stations with departures |
| **Last refresh** | 20 Oct 2026 14:44:29 (Nairobi time) |

**Conventions**

- **Net flow sign:** departures − arrivals. **Positive = the station lost bikes; negative = it gained bikes.** Every colour measure follows this: orange = losing, blue = gaining.
- **Hours** are 0–23, America/New_York local time. **Weekdays** are ISO: 1 = Monday … 7 = Sunday.
- `date_key` is an integer `YYYYMMDD`, stored as a decimal number in this model.

---

## 1. Tables

### 1.1 Source tables (ClickHouse)

Each is a straight navigation to a ClickHouse table, with no Power Query transformations.

| Table | Source | Role | Grain | Notes |
|---|---|---|---|---|
| `fact_trips` | `gold.fact_trips` | Fact (detail) | One row per ride | Base for `[Rides]` and most KPIs. Includes `start_hour` and `end_hour` columns from the source. All columns are loaded, including pipeline metadata (`_source_file`, `_ingested_at`) |
| `daily_ride_summary` | `marts.daily_ride_summary` | Fact (aggregate) | Date × rider type × bike type × weather code × daylight | Loaded and related, but **no measure uses it** |
| `mart_station_flow` | `marts.mart_station_flow` | Fact (aggregate) | Station × date × hour | Sparse: quiet hours have no row. Base for Departures, Arrivals, Net Flow and the heatmaps |
| `mart_demand_patterns` | `marts.mart_demand_patterns` | Fact (baseline) | Station × weekday × hour | "Typical" demand over all history. Joined on `station_id` |
| `mart_city_flow` | `marts.mart_city_flow` | Fact (aggregate) | Month × start city × end city | No relationships. Used only by the City Flow measures |
| `dim_date` | `gold.dim_date` | Dimension | Day | Adds `month_year` and `month_year_key` from the source, plus the calculated column `Month Solo` (a one-letter axis label) |
| `dim_station` | `gold.dim_station` | Dimension | Station version | `city` comes from the source |
| `dim_rider_type` | `gold.dim_rider_type` | Dimension | Rider type (2 rows) | |
| `dim_bike_type` | `gold.dim_bike_type` | Dimension | Bike type (3 rows) | |
| `dim_weather_code` | `gold.dim_weather_code` | Dimension | WMO code | `weather_group` is a **source column** from ClickHouse, not a DAX calculated column |

### 1.2 Helper tables (built inside the model)

| Table | How it's built | Contents | Purpose |
|---|---|---|---|
| `Measures_` | DAX calculated table `Row("Column", BLANK())` | One hidden placeholder column | Home table for all measures |
| `Ride Path` | Power Query, entered data | See below | Slicer and axis for the city-pair visual; read by `[Path Rides]` |
| `Temp Band` | Power Query, entered data | See below | Axis for the temperature-band chart; read by `[Band Rides]` |
| `Refresh` | Power Query: `DateTimeZone.UtcNow()` switched to UTC+3 | One row, `Last Refresh` | Stamp for `[Last Updated]` |
| `dim_weekday` | DAX `DATATABLE` | `day_of_week` 1–7, `Day` Monday–Sunday (sorted by `day_of_week`) | Weekday slicer for the Typical Demand pages; filters `mart_demand_patterns` and `dim_date` |
| `Deps & Arrivals` | Field parameter | `Departures` → `[Avg Departures]`, `Arrivals` → `[Avg Arrivals]` | Lets the viewer switch the typical-day chart between departures and arrivals |

**`Ride Path` rows**

| code | path | sort |
|---|---|---|
| JC-JC | Jersey City → Jersey City | 1 |
| HB-HB | Hoboken → Hoboken | 2 |
| JC-HB | Jersey City → Hoboken | 3 |
| HB-JC | Hoboken → Jersey City | 4 |
| NYC | Ends at an NYC station | 5 |

**`Temp Band` rows** (`Band` sorted by `Sort`; a ride falls in a band when `Low` ≤ temperature < `High`)

| Band | Low | High | Sort |
|---|---|---|---|
| Below 0 °C | −100 | 0 | 1 |
| 0 to 10 °C | 0 | 10 | 2 |
| 10 to 20 °C | 10 | 20 | 3 |
| 20 to 30 °C | 20 | 30 | 4 |
| Above 30 °C | 30 | 100 | 5 |

---

## 2. Measures

All measures live in the `Measures_` table. This catalog covers the 52 that calculate values; display helpers (colours, label positions, titles, subtitles and SVG visuals) are left out. They are grouped below by display folder, in model order. "Uses" lists other measures the formula calls; "Used by" lists measures that call it.

### 2.1 Index

| Folder | Measure | Format | What it returns |
|---|---|---|---|
| 01 Core KPIs | [Rides](#rides) | `#,0` | Number of rides in the current filter context. |
| 01 Core KPIs | [Rides per day](#rides-per-day) | `#,0` | Average rides per calendar date. |
| 01 Core KPIs | [Median Duration (min)](#median-duration-min) | `—` | Median ride duration in minutes. |
| 01 Core KPIs | [Median Distance (km)](#median-distance-km) | `—` | Median straight-line ride distance in km. |
| 01 Core KPIs | [Mean Temp (°C)](#mean-temp-c) | `—` | Mean hourly temperature across rides, returned as **text** (e.g. `18.4°C`) for a card. |
| 01 Core KPIs | [Stations With Rides](#stations-with-rides) | `0` | Count of distinct `station_id`s with at least one departure in context. |
| 01 Core KPIs | [Avg Rides per Day](#avg-rides-per-day) | `—` | `[Rides]` divided by `[Days With Data]`. |
| 02 Rider & Bike Mix | [Member Share %](#member-share-) | `0.0%;-0.0%;0.0%` | Member rides as a share of all rides, ignoring any rider-type filter in the denominator. |
| 02 Rider & Bike Mix | [E-Bike Share %](#e-bike-share-) | `0.0%;-0.0%;0.0%` | Electric-bike rides as a share of all rides, ignoring any bike-type filter in the denominator. |
| 02 Rider & Bike Mix | [Bike Share of Rider %](#bike-share-of-rider-) | `0.0%;-0.0%;0.0%` | Rides in the current bike type as a share of all bike types for the same rider type. |
| 02 Rider & Bike Mix | [Weekend Share %](#weekend-share-) | `0.0%;-0.0%;0.0%` | Weekend rides as a share of all rides, ignoring any weekend filter in the denominator. |
| 02 Rider & Bike Mix | [Daylight Share %](#daylight-share-) | `0.0%;-0.0%;0.0%` | Share of rides that started in daylight. |
| 03 Year over Year | [Prior Year Rides](#prior-year-rides) | `0` | Rides in the previous calendar year for the selected `dim_date[year]`. |
| 03 Year over Year | [Rides YoY %](#rides-yoy-) | `0.0%;-0.0%;0.0%` | Year-over-year change in rides: `Rides / Prior Year Rides − 1`. |
| 04 City Flow | [City Flow Rides](#city-flow-rides) | `—` | Sum of `mart_city_flow[rides]`. |
| 04 City Flow | [Path Rides](#path-rides) | `#,0.00` | Rides for the selected `'Ride Path'[code]`: JC→JC, HB→HB, JC→HB, HB→JC, or any ride ending at an NYC station. |
| 05 Weather & Temperature | [Band Rides](#band-rides) | `0` | Rides whose hourly temperature falls in the selected `'Temp Band'` range (`Low` ≤ temp < `High`). |
| 05 Weather & Temperature | [Weather Share %](#weather-share-) | `0.00%;-0.00%;0.00%` | Rides in the current weather condition as a share of rides across all weather. |
| 05 Weather & Temperature | [Clear or Cloudy %](#clear-or-cloudy-) | `0.0%;-0.0%;0.0%` | Share of rides in WMO codes 0–3 (clear sky to overcast). |
| 05 Weather & Temperature | [Band Share %](#band-share-) | `0.0%;-0.0%;0.0%` | `[Band Rides]` as a share of `[Rides]`. |
| 06 Hourly Patterns | [Weekday Peak Hour](#weekday-peak-hour) | `—` | Hour of day (0–23) with the most weekday departures in `mart_station_flow`. |
| 06 Hourly Patterns | [Weekday Morning Peak Hour](#weekday-morning-peak-hour) | `0` | Hour before noon with the most weekday departures. |
| 06 Hourly Patterns | [Weekday Hour Share %](#weekday-hour-share-) | `0.0%;-0.0%;0.0%` | Weekday departures in the current hour as a share of all weekday departures across hours. |
| 06 Hourly Patterns | [Weekend Hour Share %](#weekend-hour-share-) | `0.00%;-0.00%;0.00%` | Same for weekend departures. |
| 07 Station Flow | [Departures](#departures) | `#,0` | Sum of departures from `mart_station_flow`. |
| 07 Station Flow | [Net Flow](#net-flow) | `0` | Sum of `mart_station_flow[net_flow]` (departures − arrivals). |
| 07 Station Flow | [Largest Hour Net Flow](#largest-hour-net-flow) | `0` | Largest single station-hour net flow in context. |
| 07 Station Flow | [Busiest Hour Departures](#busiest-hour-departures) | `0` | Largest single station-hour departure count in context. |
| 07 Station Flow | [Avg Daily Net Flow](#avg-daily-net-flow) | `—` | `[Net Flow]` divided by `[Days With Data]`: average net bikes lost (+) or gained (−) per day. |
| 07 Station Flow | [Imbalance Flag](#imbalance-flag) | `0` | Returns 1 for the 5 stations with the most negative and the 5 with the most positive `[Avg Daily Net Flow]` among selected stations that have departures. |
| 07 Station Flow | [Active Days](#active-days) | `0` | Distinct dates with any station-flow row in context, ignoring the hour filter. |
| 07 Station Flow | [Net Flow Slot](#net-flow-slot) | `—` | Net flow per active day for the slot in context: `SUM(net_flow) / [Active Days]`. |
| 08 Typical Demand | [Avg Departures](#avg-departures) | `—` | Typical departures per hour: average of `mart_demand_patterns[avg_departures]`. |
| 08 Typical Demand | [Avg Arrivals](#avg-arrivals) | `—` | Typical arrivals per hour, same logic. |
| 08 Typical Demand | [Avg Net Flow](#avg-net-flow) | `—` | **Sum** of `mart_demand_patterns[avg_net_flow]`. |
| 08 Typical Demand › Range Band | [Range Low](#range-low) | `—` | Average 5th-percentile departures. |
| 08 Typical Demand › Range Band | [Range High](#range-high) | `—` | Average 95th-percentile departures. |
| 08 Typical Demand › Range Band | [Range Low Arrivals](#range-low-arrivals) | `—` | Average 5th-percentile arrivals. |
| 08 Typical Demand › Range Band | [Range High Arrivals](#range-high-arrivals) | `—` | Average 95th-percentile arrivals. |
| 08 Typical Demand › Slot Ranking | [Slot Net Flow](#slot-net-flow) | `—` | Typical net flow for one weekday × hour slot. |
| 08 Typical Demand › Slot Ranking | [Slot Sample](#slot-sample) | `—` | Number of dates behind the slot's baseline (`sample_size`). |
| 08 Typical Demand › Slot Ranking | [Is Low Baseline](#is-low-baseline) | `0` | 1 when `[Slot Sample]` is under 10 dates (unreliable baseline), else 0. |
| 08 Typical Demand › Slot Ranking | [Slot Rank](#slot-rank) | `—` | Rank of each selected station by `[Slot Rank Score]`, largest imbalance first. |
| 08 Typical Demand › Slot Ranking | [Slot Rank Score](#slot-rank-score) | `—` | Absolute slot net flow, with 1,000 subtracted for low baselines so they sink to the bottom of the ranking. |
| 09 Data Coverage | [Last Data Date](#last-data-date) | `General Date` | Latest ride date in `fact_trips`, ignoring all filters. |
| 09 Data Coverage | [First Data Date](#first-data-date) | `General Date` | Earliest ride date in `fact_trips`, ignoring all filters. |
| 09 Data Coverage | [Last Updated](#last-updated) | `—` | Time of the last model refresh (Nairobi time), from the `Refresh` table, formatted `dd mmm yyyy hh:mm:ss`. |
| 09 Data Coverage | [Days With Data](#days-with-data) | `0` | Calendar days in context that fall between the first and last data dates. |
| (none) | [Arrivals](#arrivals) | `#,0` | Sum of arrivals from `mart_station_flow`. |
| (none) | [Typical Day Departures](#typical-day-departures) | `#,0` | Total typical departures across all hours for the selected weekday (sum of hourly averages). |
| (none) | [Evening Net Flow](#evening-net-flow) | `—` | Typical net flow at the busiest afternoon/evening hour (noon onward) for the selected weekday. |
| (none) | [Avg Duration (min)](#avg-duration-min) | `—` | Mean ride duration in minutes from `fact_trips`. |

### 2.2 01 Core KPIs

#### Rides

Number of rides in the current filter context. Counts rows in `fact_trips`, so it responds to every dimension; station filters apply to the **start** station.

**Format:** `#,0` · **Used by:** `Avg Rides per Day`, `Band Rides`, `Band Share %`, `Bike Share of Rider %`, `Clear or Cloudy %`, `Daylight Share %`, `E-Bike Share %`, `Member Share %`, `Prior Year Rides`, `Rides YoY %`, `Rides per day`, `Weather Share %`, `Weekend Share %`

```dax
Rides =
COUNTROWS(fact_trips)
```

#### Rides per day

Average rides per calendar date. Iterates the dates in context and averages `[Rides]`; dates with no rides return blank and are skipped.

**Format:** `#,0` · **Uses:** `Rides`

```dax
Rides per day =
AVERAGEX(
    VALUES(dim_date[full_date]),
    [Rides]
)
```

#### Median Duration (min)

Median ride duration in minutes. Used instead of the mean because durations are skewed (up to 24 h).

**Format:** none set

```dax
Median Duration (min) =
MEDIAN ( fact_trips[ride_duration_minutes] )
```

#### Median Distance (km)

Median straight-line ride distance in km. Rides with no end coordinates (null distance) are ignored.

**Format:** none set

```dax
Median Distance (km) =
MEDIAN ( fact_trips[ride_distance_km] )
```

#### Mean Temp (°C)

Mean hourly temperature across rides, returned as **text** (e.g. `18.4°C`) for a card. Cannot be used as a number.

**Format:** none set

```dax
Mean Temp (°C) =
VAR __Avg = AVERAGE ( fact_trips[temperature_c] )
RETURN FORMAT ( __Avg, "0.0" ) & "°C"
```

#### Stations With Rides

Count of distinct `station_id`s with at least one departure in context.

**Format:** `0` · **Uses:** `Departures`

```dax
Stations With Rides =
COUNTROWS ( FILTER ( VALUES ( dim_station[station_id] ), [Departures] > 0 ) )
```

#### Avg Rides per Day

`[Rides]` divided by `[Days With Data]`. Unlike `Rides per day`, days with zero rides count in the denominator.

**Format:** none set · **Uses:** `Days With Data`, `Rides`

```dax
Avg Rides per Day =
DIVIDE ( [Rides], [Days With Data] )
```

### 2.3 02 Rider & Bike Mix

#### Member Share %

Member rides as a share of all rides, ignoring any rider-type filter in the denominator.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Rides`

```dax
Member Share % =
DIVIDE (
    CALCULATE (
        [Rides],
        dim_rider_type[rider_type] = "member"
    ),
    CALCULATE (
        [Rides],
        REMOVEFILTERS ( dim_rider_type )
    )
)
```

#### E-Bike Share %

Electric-bike rides as a share of all rides, ignoring any bike-type filter in the denominator.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Rides`

```dax
E-Bike Share % =
DIVIDE (
    CALCULATE (
        [Rides],
        dim_bike_type[bike_type] = "electric_bike"
    ),
    CALCULATE (
        [Rides],
        REMOVEFILTERS ( dim_bike_type )
    )
)
```

#### Bike Share of Rider %

Rides in the current bike type as a share of all bike types for the same rider type. Built for a rider × bike breakdown.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Rides`

```dax
Bike Share of Rider % =
DIVIDE (
    [Rides],
    CALCULATE ( [Rides], REMOVEFILTERS ( dim_bike_type ) )
)
```

#### Weekend Share %

Weekend rides as a share of all rides, ignoring any weekend filter in the denominator.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Rides`

```dax
Weekend Share % =
DIVIDE (
    CALCULATE ( [Rides], dim_date[is_weekend] = 1 ),
    CALCULATE ( [Rides], REMOVEFILTERS ( dim_date[is_weekend] ) )
)
```

#### Daylight Share %

Share of rides that started in daylight.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Rides`

```dax
Daylight Share % =
DIVIDE ( CALCULATE ( [Rides], fact_trips[is_daylight] = TRUE () ), [Rides] )
```

### 2.4 03 Year over Year

#### Prior Year Rides

Rides in the previous calendar year for the selected `dim_date[year]`. If the selected year is the current, incomplete year, the prior year is cut off at the same point (last data date minus 12 months), so YoY compares like for like. Blank when no single year is in context.

**Format:** `0` · **Uses:** `Last Data Date`, `Rides` · **Used by:** `Rides YoY %`

```dax
Prior Year Rides =
VAR y        = SELECTEDVALUE ( dim_date[year] )
VAR _lastDate = [Last Data Date]
VAR partial  = y = YEAR ( _lastDate ) && _lastDate < DATE ( y, 12, 31 )
VAR cutoff   = EDATE ( _lastDate, -12 )
RETURN
    IF (
        NOT ISBLANK ( y ),
        CALCULATE (
            [Rides],
            FILTER (
                ALL ( dim_date ),
                dim_date[year] = y - 1
                    && ( NOT partial || dim_date[full_date] <= cutoff )
            )
        )
    )
```

#### Rides YoY %

Year-over-year change in rides: `Rides / Prior Year Rides − 1`. Blank when there is no prior year.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Prior Year Rides`, `Rides`

```dax
Rides YoY % =
VAR prior = [Prior Year Rides]
RETURN IF ( NOT ISBLANK ( prior ) && prior <> 0, DIVIDE ( [Rides], prior ) - 1 )
```

### 2.5 04 City Flow

#### City Flow Rides

Sum of `mart_city_flow[rides]`. Base measure for the city-pair visuals.

**Format:** none set · **Used by:** `Path Rides`

```dax
City Flow Rides =
SUM ( mart_city_flow[rides] )
```

#### Path Rides

Rides for the selected `'Ride Path'[code]`: JC→JC, HB→HB, JC→HB, HB→JC, or any ride ending at an NYC station. Uses `mart_city_flow` filtered by start and end city.

**Format:** `#,0.00` · **Uses:** `City Flow Rides`

```dax
Path Rides =
    SWITCH(
        SELECTEDVALUE( 'Ride Path'[code] ),
        "JC-JC", CALCULATE ( [City Flow Rides], mart_city_flow[start_city] = "Jersey City", mart_city_flow[end_city] = "Jersey City" ),
        "HB-HB", CALCULATE ( [City Flow Rides], mart_city_flow[start_city] = "Hoboken", mart_city_flow[end_city] = "Hoboken" ),
        "JC-HB", CALCULATE ( [City Flow Rides], mart_city_flow[start_city] = "Jersey City", mart_city_flow[end_city] = "Hoboken" ),
        "HB-JC", CALCULATE ( [City Flow Rides], mart_city_flow[start_city] = "Hoboken", mart_city_flow[end_city] = "Jersey City" ),
        "NYC",   CALCULATE ( [City Flow Rides], mart_city_flow[end_city] = "New York City" )
    )
```

### 2.6 05 Weather & Temperature

#### Band Rides

Rides whose hourly temperature falls in the selected `'Temp Band'` range (`Low` ≤ temp < `High`).

**Format:** `0` · **Uses:** `Rides` · **Used by:** `Band Share %`

```dax
Band Rides =
VAR lo = MIN ( 'Temp Band'[Low] )
VAR hi = MAX ( 'Temp Band'[High] )
RETURN
    CALCULATE (
        [Rides],
        fact_trips[temperature_c] >= lo,
        fact_trips[temperature_c] < hi
    )
```

#### Weather Share %

Rides in the current weather condition as a share of rides across all weather.

**Format:** `0.00%;-0.00%;0.00%` · **Uses:** `Rides`

```dax
Weather Share % =
DIVIDE (
    [Rides],
    CALCULATE ( [Rides], REMOVEFILTERS ( dim_weather_code ) )
)
```

#### Clear or Cloudy %

Share of rides in WMO codes 0–3 (clear sky to overcast).

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Rides`

```dax
Clear or Cloudy % =
DIVIDE ( CALCULATE ( [Rides], dim_weather_code[weather_code] <= 3 ), [Rides] )
```

#### Band Share %

`[Band Rides]` as a share of `[Rides]`.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Band Rides`, `Rides`

```dax
Band Share % =
DIVIDE ( [Band Rides], [Rides] )
```

### 2.7 06 Hourly Patterns

#### Weekday Peak Hour

Hour of day (0–23) with the most weekday departures in `mart_station_flow`.

**Format:** none set · **Uses:** `Departures`

```dax
Weekday Peak Hour =
VAR byHour =
    ADDCOLUMNS (
        ALL ( mart_station_flow[hour] ),
        "@d", CALCULATE ( [Departures], dim_date[is_weekend] = 0 )
    )
RETURN MAXX ( TOPN ( 1, byHour, [@d], DESC ), mart_station_flow[hour] )
```

#### Weekday Morning Peak Hour

Hour before noon with the most weekday departures.

**Format:** `0` · **Uses:** `Departures`

```dax
Weekday Morning Peak Hour =
VAR byHour =
    ADDCOLUMNS (
        FILTER ( ALL ( mart_station_flow[hour] ), mart_station_flow[hour] < 12 ),
        "@d", CALCULATE ( [Departures], dim_date[is_weekend] = 0 )
    )
RETURN MAXX ( TOPN ( 1, byHour, [@d], DESC ), mart_station_flow[hour] )
```

#### Weekday Hour Share %

Weekday departures in the current hour as a share of all weekday departures across hours.

**Format:** `0.0%;-0.0%;0.0%` · **Uses:** `Departures`

```dax
Weekday Hour Share % =
DIVIDE (
    CALCULATE ( [Departures], dim_date[is_weekend] = 0 ),
    CALCULATE ( [Departures], dim_date[is_weekend] = 0, REMOVEFILTERS ( mart_station_flow[hour] ) )
)
```

#### Weekend Hour Share %

Same for weekend departures.

**Format:** `0.00%;-0.00%;0.00%` · **Uses:** `Departures`

```dax
Weekend Hour Share % =
DIVIDE (
    CALCULATE ( [Departures], dim_date[is_weekend] = 1 ),
    CALCULATE ( [Departures], dim_date[is_weekend] = 1, REMOVEFILTERS ( mart_station_flow[hour] ) )
)
```

### 2.8 07 Station Flow

#### Departures

Sum of departures from `mart_station_flow`.

**Format:** `#,0` · **Used by:** `Imbalance Flag`, `Stations With Rides`, `Weekday Hour Share %`, `Weekday Morning Peak Hour`, `Weekday Peak Hour`, `Weekend Hour Share %`

```dax
Departures =
SUM ( mart_station_flow[departures] )
```

#### Net Flow

Sum of `mart_station_flow[net_flow]` (departures − arrivals). **Positive = the station lost bikes.**

**Format:** `0` · **Used by:** `Avg Daily Net Flow`

```dax
Net Flow =
SUM ( mart_station_flow[net_flow] )
```

#### Largest Hour Net Flow

Largest single station-hour net flow in context.

**Format:** `0`

```dax
Largest Hour Net Flow =
MAX ( mart_station_flow[net_flow] )
```

#### Busiest Hour Departures

Largest single station-hour departure count in context.

**Format:** `0`

```dax
Busiest Hour Departures =
MAX ( mart_station_flow[departures] )
```

#### Avg Daily Net Flow

`[Net Flow]` divided by `[Days With Data]`: average net bikes lost (+) or gained (−) per day.

**Format:** none set · **Uses:** `Days With Data`, `Net Flow` · **Used by:** `Imbalance Flag`

```dax
Avg Daily Net Flow =
DIVIDE ( [Net Flow], [Days With Data] )
```

#### Imbalance Flag

Returns 1 for the 5 stations with the most negative and the 5 with the most positive `[Avg Daily Net Flow]` among selected stations that have departures. Used as a visual filter.

**Format:** `0` · **Uses:** `Avg Daily Net Flow`, `Departures`

```dax
Imbalance Flag =
VAR pool = FILTER ( ALLSELECTED ( dim_station ), [Departures] > 0 )
VAR v    = [Avg Daily Net Flow]
VAR low  = RANKX ( pool, [Avg Daily Net Flow], v, ASC )
VAR high = RANKX ( pool, [Avg Daily Net Flow], v, DESC )
RETURN IF ( NOT ISBLANK ( v ) && ( low <= 5 || high <= 5 ), 1 )
```

#### Active Days

Distinct dates with any station-flow row in context, ignoring the hour filter.

**Format:** `0` · **Used by:** `Net Flow Slot`

```dax
Active Days =
CALCULATE (
    DISTINCTCOUNT ( mart_station_flow[date_key] ),
    REMOVEFILTERS ( mart_station_flow[hour] )
)
```

#### Net Flow Slot

Net flow per active day for the slot in context: `SUM(net_flow) / [Active Days]`. Feeds the net-flow heatmap.

**Format:** none set · **Uses:** `Active Days`

```dax
Net Flow Slot =
DIVIDE ( SUM ( mart_station_flow[net_flow] ), [Active Days] )
```

### 2.9 08 Typical Demand

#### Avg Departures

Typical departures per hour: average of `mart_demand_patterns[avg_departures]`. Exact for one station × weekday × hour; an average of averages otherwise.

**Format:** none set

```dax
Avg Departures =
AVERAGE ( mart_demand_patterns[avg_departures] )
```

#### Avg Arrivals

Typical arrivals per hour, same logic.

**Format:** none set

```dax
Avg Arrivals =
AVERAGE ( mart_demand_patterns[avg_arrivals] )
```

#### Avg Net Flow

**Sum** of `mart_demand_patterns[avg_net_flow]`. For one station and weekday, across the hours shown, this gives the typical net bikes lost per day; across several stations it adds them up.

**Format:** none set

```dax
Avg Net Flow =
SUM ( mart_demand_patterns[avg_net_flow] )
```

### 2.10 08 Typical Demand › Range Band

#### Range Low

Average 5th-percentile departures. Lower edge of the typical-range band.

**Format:** none set

```dax
Range Low =
AVERAGE ( mart_demand_patterns[p05_departures] )
```

#### Range High

Average 95th-percentile departures. Upper edge of the band.

**Format:** none set

```dax
Range High =
AVERAGE ( mart_demand_patterns[p95_departures] )
```

#### Range Low Arrivals

Average 5th-percentile arrivals.

**Format:** none set

```dax
Range Low Arrivals =
AVERAGE ( mart_demand_patterns[p05_arrivals] )
```

#### Range High Arrivals

Average 95th-percentile arrivals.

**Format:** none set

```dax
Range High Arrivals =
AVERAGE ( mart_demand_patterns[p95_arrivals] )
```

### 2.11 08 Typical Demand › Slot Ranking

#### Slot Net Flow

Typical net flow for one weekday × hour slot. Blank unless exactly one weekday and one hour are in context.

**Format:** none set · **Used by:** `Slot Rank`, `Slot Rank Score`

```dax
Slot Net Flow =
IF (
    HASONEVALUE ( mart_demand_patterns[day_of_week] ) && HASONEVALUE ( mart_demand_patterns[hour] ),
    SUM ( mart_demand_patterns[avg_net_flow] )
)
```

#### Slot Sample

Number of dates behind the slot's baseline (`sample_size`). Blank unless one weekday and one hour are in context.

**Format:** none set · **Used by:** `Is Low Baseline`

```dax
Slot Sample =
IF (
    HASONEVALUE ( mart_demand_patterns[day_of_week] ) && HASONEVALUE ( mart_demand_patterns[hour] ),
    MAX ( mart_demand_patterns[sample_size] )
)
```

#### Is Low Baseline

1 when `[Slot Sample]` is under 10 dates (unreliable baseline), else 0.

**Format:** `0` · **Uses:** `Slot Sample` · **Used by:** `Slot Rank Score`

```dax
Is Low Baseline =
IF ( [Slot Sample] < 10, 1, 0 )
```

#### Slot Rank

Rank of each selected station by `[Slot Rank Score]`, largest imbalance first.

**Format:** none set · **Uses:** `Slot Net Flow`, `Slot Rank Score`

```dax
Slot Rank =
RANKX ( FILTER ( ALLSELECTED ( dim_station ), NOT ISBLANK ( [Slot Net Flow] ) ), [Slot Rank Score], , DESC )
```

#### Slot Rank Score

Absolute slot net flow, with 1,000 subtracted for low baselines so they sink to the bottom of the ranking.

**Format:** none set · **Uses:** `Is Low Baseline`, `Slot Net Flow` · **Used by:** `Slot Rank`

```dax
Slot Rank Score =
IF ( [Is Low Baseline] = 1, -1000, 0 ) + ABS ( [Slot Net Flow] )
```

### 2.12 09 Data Coverage

#### Last Data Date

Latest ride date in `fact_trips`, ignoring all filters. Converts the `YYYYMMDD` `date_key` into a date.

**Format:** `General Date` · **Used by:** `Days With Data`, `Prior Year Rides`

```dax
Last Data Date =
VAR k = CALCULATE ( MAX ( fact_trips[date_key] ), REMOVEFILTERS () )
RETURN DATE ( INT ( k / 10000 ), MOD ( INT ( k / 100 ), 100 ), MOD ( k, 100 ) )
```

#### First Data Date

Earliest ride date in `fact_trips`, ignoring all filters.

**Format:** `General Date` · **Used by:** `Days With Data`

```dax
First Data Date =
VAR k = CALCULATE ( MIN ( fact_trips[date_key] ), REMOVEFILTERS () )
RETURN DATE ( INT ( k / 10000 ), MOD ( INT ( k / 100 ), 100 ), MOD ( k, 100 ) )
```

#### Last Updated

Time of the last model refresh (Nairobi time), from the `Refresh` table, formatted `dd mmm yyyy hh:mm:ss`.

**Format:** none set

```dax
Last Updated =
FORMAT( MAX( 'Refresh'[Last Refresh] ), "dd mmm yyyy hh:mm:ss")
```

#### Days With Data

Calendar days in context that fall between the first and last data dates.

**Format:** `0` · **Uses:** `First Data Date`, `Last Data Date` · **Used by:** `Avg Daily Net Flow`, `Avg Rides per Day`

```dax
Days With Data =
VAR firstD = [First Data Date]
VAR lastD  = [Last Data Date]
RETURN
    CALCULATE (
        COUNTROWS ( dim_date ),
        KEEPFILTERS ( dim_date[full_date] >= firstD ),
        KEEPFILTERS ( dim_date[full_date] <= lastD )
    )
```

### 2.13 No display folder

> These measures sit at the root of `Measures_`. Consider moving them into the matching folders (suggestions in section 7).

#### Arrivals

Sum of arrivals from `mart_station_flow`.

**Format:** `#,0`

```dax
Arrivals =
SUM ( mart_station_flow[arrivals] )
```

#### Typical Day Departures

Total typical departures across all hours for the selected weekday (sum of hourly averages). Blank unless one weekday is selected.

**Format:** `#,0`

```dax
Typical Day Departures =
IF (
    HASONEVALUE ( dim_weekday[Day] ),
    CALCULATE ( SUM ( mart_demand_patterns[avg_departures] ), REMOVEFILTERS ( mart_demand_patterns[hour] ) )
)
```

#### Evening Net Flow

Typical net flow at the busiest afternoon/evening hour (noon onward) for the selected weekday.

**Format:** none set

```dax
Evening Net Flow =
IF (
    HASONEVALUE ( dim_weekday[Day] ),
    VAR t =
        ADDCOLUMNS (
            FILTER ( ALL ( mart_demand_patterns[hour] ), mart_demand_patterns[hour] >= 12 ),
            "@d", CALCULATE ( SUM ( mart_demand_patterns[avg_departures] ) ),
            "@n", CALCULATE ( SUM ( mart_demand_patterns[avg_net_flow] ) )
        )
    VAR best = TOPN ( 1, t, [@d], DESC )
    RETURN
        MAXX ( best, [@n] )
)
```

#### Avg Duration (min)

Mean ride duration in minutes from `fact_trips`. Pulled up by very long rides; `Median Duration (min)` is the typical value.

**Format:** none set

```dax
Avg Duration (min) =
AVERAGE ( fact_trips[ride_duration_minutes] )
```

---

## 3. Relationships

All are many-to-one, single direction (dimension filters fact) and active, unless the table says otherwise.

| Many side (from) | One side (to) | Notes |
|---|---|---|
| `fact_trips[date_key]` | `dim_date[date_key]` | |
| `fact_trips[start_station_key]` | `dim_station[station_key]` | Start station only |
| `fact_trips[rider_type_key]` | `dim_rider_type[rider_type_key]` | |
| `fact_trips[bike_type_key]` | `dim_bike_type[bike_type_key]` | |
| `fact_trips[weather_code]` | `dim_weather_code[weather_code]` | |
| `daily_ride_summary[date_key]` | `dim_date[date_key]` | |
| `daily_ride_summary[rider_type_key]` | `dim_rider_type[rider_type_key]` | |
| `daily_ride_summary[bike_type_key]` | `dim_bike_type[bike_type_key]` | |
| `daily_ride_summary[weather_code]` | `dim_weather_code[weather_code]` | |
| `mart_station_flow[date_key]` | `dim_date[date_key]` | |
| `mart_station_flow[station_key]` | `dim_station[station_key]` | |
| `mart_demand_patterns[station_id]` | `dim_station[station_id]` | Needs `station_id` to stay unique in `dim_station` |
| `mart_demand_patterns[day_of_week]` | `dim_weekday[day_of_week]` | |
| `dim_date[day_of_week_num]` | `dim_weekday[day_of_week]` | Snowflake: a weekday slicer also filters every fact table through `dim_date` |
| `'Temp Band'[Sort]` | `'Ride Path'[sort]` | **One-to-one, both directions, auto-detected.** See section 7 |
