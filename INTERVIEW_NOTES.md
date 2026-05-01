# Interview Notes

## 60-second pitch

I built a local ELT pipeline that combines GP online consultation requests with historical weather data. It loads and validates raw JSON files, geocodes cities, fetches historical daily weather, and builds a DuckDB analytical mart at city/day grain. The final table lets a data scientist test whether medical requests increase on poor-weather days.

I also added production-minded features: rejection tables, deduplication, retries, incremental enrichment, data-quality checks, a mocked integration test, security notes, manual geocode overrides, and operational run metadata.

## The final table grain

One row per city per request date.

This matters because every metric in the mart must be interpreted at that grain. If someone wants hourly staffing analysis, that should be a separate mart.

## Key decisions to defend

- DuckDB keeps the project local, inspectable, and SQL-first.
- Raw, staging, and mart layers separate source preservation from analytical modelling.
- Weather is daily because the hypothesis is daily.
- Weather is left joined so consultation activity is not dropped when enrichment is missing.
- `is_poor_weather` is configurable because the threshold is subjective.
- City geocoding uses API results plus manual override support because city names can be ambiguous.
- Checks fail fast on missing prerequisite tables so operators get an actionable message.

## Likely questions

### Why not hourly weather?

The business hypothesis is daily demand versus daily weather. Hourly weather would be useful for intra-day operations, but it would add complexity and noise for this question.

### What happens if Open-Meteo is down?

The HTTP helper retries transient failures. Persistent geocode and weather failures are logged to raw failure tables. A production version should also cache raw API responses and alert on high failure rates.

### What if a city is geocoded incorrectly?

The current ranking prefers GB and larger population matches. I added CSV override support for manual corrections. Production should use a governed city dimension with stable IDs and confidence review.

### How do you know the mart is fresh?

The checks now verify that the mart date range matches raw consultations and that weather date range covers the consultation date range.

### What is the biggest production gap?

Orchestration and governed observability. The current script is clear and runnable, but production should use dbt, an orchestrator, alerting, API response caching, and a curated location dimension.

## Strong closing answer

I focused on making the pipeline understandable, rerunnable, testable, and honest about data quality. The analytical table is simple, but the surrounding design choices make it reliable: validation, rejection tables, retries, incremental enrichment, left joins to preserve facts, explicit checks, manual geocode overrides, and run metadata.
