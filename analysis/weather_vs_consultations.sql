/*
  Example analysis queries for marts.fct_daily_consultation_weather.

  These queries do not prove causation. They are starting points for a data
  scientist to inspect whether poor-weather days are associated with different
  medical request volumes.
*/

-- 1. Overall request volume on poor-weather vs non-poor-weather days.
SELECT
    is_poor_weather,
    COUNT(*) AS city_days,
    ROUND(AVG(total_requests), 2) AS avg_total_requests,
    ROUND(AVG(medical_requests), 2) AS avg_medical_requests,
    ROUND(AVG(medical_pct), 2) AS avg_medical_pct
FROM marts.fct_daily_consultation_weather
WHERE is_poor_weather IS NOT NULL
GROUP BY is_poor_weather
ORDER BY is_poor_weather DESC;

-- 2. Same comparison, split by weekday/weekend to account for a major confounder.
SELECT
    CASE WHEN day_name IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    is_poor_weather,
    COUNT(*) AS city_days,
    ROUND(AVG(medical_requests), 2) AS avg_medical_requests,
    ROUND(AVG(medical_pct), 2) AS avg_medical_pct
FROM marts.fct_daily_consultation_weather
WHERE is_poor_weather IS NOT NULL
GROUP BY day_type, is_poor_weather
ORDER BY day_type, is_poor_weather DESC;

-- 3. Cities with the strongest observed difference in average medical requests.
WITH city_weather_split AS (
    SELECT
        city,
        AVG(medical_requests) FILTER (WHERE is_poor_weather) AS avg_medical_poor_weather,
        AVG(medical_requests) FILTER (WHERE NOT is_poor_weather) AS avg_medical_normal_weather,
        COUNT(*) FILTER (WHERE is_poor_weather) AS poor_weather_days,
        COUNT(*) FILTER (WHERE NOT is_poor_weather) AS normal_weather_days
    FROM marts.fct_daily_consultation_weather
    WHERE is_poor_weather IS NOT NULL
    GROUP BY city
)
SELECT
    city,
    poor_weather_days,
    normal_weather_days,
    ROUND(avg_medical_poor_weather, 2) AS avg_medical_poor_weather,
    ROUND(avg_medical_normal_weather, 2) AS avg_medical_normal_weather,
    ROUND(avg_medical_poor_weather - avg_medical_normal_weather, 2) AS avg_medical_request_diff
FROM city_weather_split
WHERE poor_weather_days >= 10
  AND normal_weather_days >= 10
ORDER BY avg_medical_request_diff DESC
LIMIT 20;

-- 4. Weather coverage by city.
SELECT
    city,
    COUNT(*) AS city_days,
    COUNT(is_poor_weather) AS city_days_with_weather,
    ROUND(COUNT(is_poor_weather) * 100.0 / NULLIF(COUNT(*), 0), 1) AS weather_coverage_pct
FROM marts.fct_daily_consultation_weather
GROUP BY city
ORDER BY weather_coverage_pct ASC, city_days DESC
LIMIT 25;
