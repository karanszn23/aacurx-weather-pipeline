# Data Dictionary

## Final mart: `marts.fct_daily_consultation_weather`

Grain: one row per `city` and `request_date`.

Primary use: analyse whether medical request volume differs on poor-weather days.

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `city` | VARCHAR | Consultation city after trimming. | String key; production should use a stable city/location ID. |
| `request_date` | DATE | Europe/London local date of the consultation request. | Derived from source timestamp. |
| `day_of_week` | INTEGER | DuckDB day-of-week number. | Useful confounder for weekday/weekend patterns. |
| `day_name` | VARCHAR | Name of the request day. | Human-readable companion to `day_of_week`. |
| `total_requests` | BIGINT | Total consultation requests for the city/date. | Equals medical plus admin requests. |
| `medical_requests` | BIGINT | Count of requests with `request_type = 'Medical'`. | Main metric for the weather hypothesis. |
| `admin_requests` | BIGINT | Count of requests with `request_type = 'Admin'`. | Included for reconciliation and mix analysis. |
| `medical_pct` | DOUBLE | Medical requests as a percentage of total requests. | Rounded to 1 decimal place. |
| `temp_max` | DOUBLE | Daily maximum temperature from Open-Meteo. | Null when weather enrichment is missing. |
| `temp_min` | DOUBLE | Daily minimum temperature from Open-Meteo. | Null when weather enrichment is missing. |
| `temp_mean` | DOUBLE | Daily mean temperature from Open-Meteo. | Null when weather enrichment is missing. |
| `precipitation_mm` | DOUBLE | Daily total precipitation in millimetres. | Used in poor-weather classification. |
| `rain_mm` | DOUBLE | Daily rain amount in millimetres. | Kept for analyst flexibility. |
| `wind_max_kmh` | DOUBLE | Maximum daily wind speed in km/h. | Used in poor-weather classification. |
| `weather_code` | INTEGER | WMO daily weather code. | Used in poor-weather classification. |
| `is_poor_weather` | BOOLEAN | Derived flag for rain/snow/storm codes, heavy precipitation, or high wind. | Subjective and configurable via environment variables. |

## Operational tables

### `ops.pipeline_runs`

One row per full pipeline run.

| Column | Meaning |
|---|---|
| `run_id` | UUID for correlating run metadata and check results. |
| `started_at` | Run start timestamp. |
| `finished_at` | Run completion timestamp. |
| `status` | `running`, `succeeded`, or `failed`. |
| `full_refresh` | Whether raw caches were cleared before running. |
| `skip_checks` | Whether final data checks were skipped. |
| `error_message` | Failure message or stack trace when available. |
| `consultation_rows` | Row count in `raw.consultations` at completion. |
| `weather_rows` | Row count in `raw.weather` at completion. |
| `mart_rows` | Row count in the final mart at completion. |

### `ops.data_check_results`

One row per check result for a run.

| Column | Meaning |
|---|---|
| `run_id` | Pipeline run ID. |
| `check_name` | Human-readable check name. |
| `status` | `pass` or `fail`. |
| `bad_row_count` | Number of failing rows where applicable. |
| `error_message` | Query or prerequisite error where applicable. |
| `checked_at` | Check execution timestamp. |

## Important caveats

- Weather is daily, not hourly.
- Weather is city-level, not exact patient/practice location-level.
- `is_poor_weather` is intentionally configurable and should be sensitivity-tested.
- The mart supports correlation analysis; it does not prove causation on its own.
