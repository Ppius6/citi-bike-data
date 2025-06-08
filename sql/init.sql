-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
-- Raw trips table
CREATE TABLE IF NOT EXISTS raw.trips (
    ride_id VARCHAR(50) PRIMARY KEY,
    rideable_type VARCHAR(50),
    started_at TIMESTAMP WITH TIME ZONE,
    ended_at TIMESTAMP WITH TIME ZONE,
    start_station_name VARCHAR(255),
    start_station_id VARCHAR(50),
    end_station_name VARCHAR(255),
    end_station_id VARCHAR(50),
    start_lat DECIMAL(10, 8),
    start_lng DECIMAL(11, 8),
    end_lat DECIMAL(10, 8),
    end_lng DECIMAL(11, 8),
    member_casual VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_trips_started_at ON raw.trips(started_at);
CREATE INDEX IF NOT EXISTS idx_trips_start_station ON raw.trips(start_station_id);
CREATE INDEX IF NOT EXISTS idx_trips_end_station ON raw.trips(end_station_id);
CREATE INDEX IF NOT EXISTS idx_trips_member_type ON raw.trips(member_casual);
-- Create staging tables
CREATE TABLE IF NOT EXISTS staging.trips_cleaned AS
SELECT *
FROM raw.trips
WHERE 1 = 0;
-- Create dimension tables
CREATE TABLE IF NOT EXISTS marts.dim_stations (
    station_key SERIAL PRIMARY KEY,
    station_id VARCHAR(50) UNIQUE,
    station_name VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS marts.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);
CREATE TABLE IF NOT EXISTS marts.fact_trips (
    trip_key SERIAL PRIMARY KEY,
    ride_id VARCHAR(50) UNIQUE,
    start_station_key INTEGER REFERENCES marts.dim_stations(station_key),
    end_station_key INTEGER REFERENCES marts.dim_stations(station_key),
    start_date_key INTEGER REFERENCES marts.dim_date(date_key),
    end_date_key INTEGER REFERENCES marts.dim_date(date_key),
    rideable_type VARCHAR(50),
    member_casual VARCHAR(20),
    trip_duration_minutes INTEGER,
    trip_distance_km DECIMAL(8, 3),
    start_hour INTEGER,
    end_hour INTEGER,
    is_rush_hour BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Insert sample date dimension data
INSERT INTO marts.dim_date (
        date_key,
        date,
        year,
        month,
        day,
        quarter,
        day_of_week,
        day_name,
        month_name,
        is_weekend,
        is_holiday
    )
SELECT TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,
    date_series::DATE as date,
    EXTRACT(
        YEAR
        FROM date_series
    ) as year,
    EXTRACT(
        MONTH
        FROM date_series
    ) as month,
    EXTRACT(
        DAY
        FROM date_series
    ) as day,
    EXTRACT(
        QUARTER
        FROM date_series
    ) as quarter,
    EXTRACT(
        DOW
        FROM date_series
    ) as day_of_week,
    TO_CHAR(date_series, 'Day') as day_name,
    TO_CHAR(date_series, 'Month') as month_name,
    EXTRACT(
        DOW
        FROM date_series
    ) IN (0, 6) as is_weekend,
    FALSE as is_holiday
FROM generate_series('2021-01-01'::DATE, '2025-12-31'::DATE, '1 day') as date_series ON CONFLICT (date_key) DO NOTHING;