-- stg_consultations: light cleaning on raw consultations

CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.stg_consultations AS
SELECT
    CAST(id AS BIGINT) AS consultation_id,
    TRIM(city) AS city,
    timestamp AS requested_at,
    request_date,
    TRIM(request_type) AS request_type
FROM raw.consultations
WHERE TRIM(request_type) IN ('Medical', 'Admin');
