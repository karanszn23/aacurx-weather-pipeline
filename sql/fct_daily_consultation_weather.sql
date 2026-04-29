/*
  fct_daily_consultation_weather

  One row per city per day.
  Joins consultation counts with weather so the data scientist can test
  whether medical requests increase on poor weather days.

  LEFT JOIN so we keep consultation data even when weather is missing.
  day_of_week included because weekday vs weekend is a confounder.
*/

CREATE SCHEMA IF NOT EXISTS marts;

CREATE OR REPLACE TABLE marts.fct_daily_consultation_weather AS

WITH daily_counts AS (
    SELECT
        city,
        request_date,
        COUNT(*) AS total_requests,
        COUNT(*) FILTER (WHERE request_type = 'Medical') AS medical_requests,
        COUNT(*) FILTER (WHERE request_type = 'Admin') AS admin_requests
    FROM staging.stg_consultations
    GROUP BY city, request_date
)

SELECT
    c.city,
    c.request_date,
    DAYOFWEEK(c.request_date) AS day_of_week,
    DAYNAME(c.request_date) AS day_name,
    c.total_requests,
    c.medical_requests,
    c.admin_requests,
    ROUND(c.medical_requests * 100.0 / NULLIF(c.total_requests, 0), 1) AS medical_pct,
    w.temp_max,
    w.temp_min,
    w.temp_mean,
    w.precipitation_mm,
    w.rain_mm,
    w.wind_max_kmh,
    w.weather_code,
    w.is_poor_weather
FROM daily_counts c
LEFT JOIN staging.stg_weather w
    ON c.city = w.city
    AND c.request_date = w.weather_date
ORDER BY c.city, c.request_date;
