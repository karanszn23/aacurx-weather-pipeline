-- stg_weather: cast types, rename date column for clarity

CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.stg_weather AS
SELECT
    city,
    CAST(date AS DATE) AS weather_date,
    temp_max,
    temp_min,
    temp_mean,
    precipitation_mm,
    rain_mm,
    wind_max_kmh,
    CAST(weather_code AS INT) AS weather_code,
    is_poor_weather
FROM raw.weather;
