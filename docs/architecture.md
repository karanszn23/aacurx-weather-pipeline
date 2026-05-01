# Architecture and Design Notes

## Product goal

Create an analysis-ready daily dataset that lets a data scientist test whether poor weather is associated with higher GP online medical request volume.

The important product decision is that this repo is not just a data mover. It is a small data product with:

- a clear analytical grain;
- documented assumptions;
- quality checks;
- lineage and rejection tables;
- operational run metadata;
- a roadmap for production hardening.

## Data flow

```text
data/<date>/<hour>.json
        |
        v
raw.consultations
raw.consultation_rejections
raw.consultation_ingested_files
        |
        v
raw.city_coordinates
raw.city_geocode_failures
        |
        v
raw.weather
raw.weather_fetch_failures
        |
        v
staging.stg_consultations
staging.stg_weather
        |
        v
marts.fct_daily_consultation_weather
        |
        v
ops.pipeline_runs
ops.data_check_results
```

## Key design decisions

### DuckDB as the warehouse

DuckDB is enough for the take-home scale and gives a warehouse-like SQL modelling experience without cloud setup. It is easy to inspect locally and portable as a single file.

Production tradeoff: DuckDB is not the right final storage layer for multi-user, governed analytics at larger scale. A production version would likely move to a managed warehouse or lakehouse.

### Daily mart grain

The mart is one row per city/day because the hypothesis is about daily weather and daily consultation demand.

Production tradeoff: if operations need intra-day staffing, add an hourly mart instead of overloading the daily mart.

### Preserve consultation facts

The final model left joins weather onto consultation counts. This preserves consultation rows even when weather enrichment is missing.

Production tradeoff: downstream users must handle null weather metrics and monitor coverage.

### Manual geocode overrides

API geocoding is convenient, but city names can be ambiguous. The override CSV gives a lightweight path for curated corrections without building a full dimension-management system.

Production tradeoff: a mature system should replace this with a governed city dimension and review workflow.

## Quality gates

The checks are designed to catch:

- duplicate consultation IDs;
- unexpected request types;
- null critical fields;
- duplicate weather grain rows;
- stale mart date ranges;
- stale weather date ranges;
- broken request-count arithmetic;
- invalid percentages;
- negative counts;
- low weather coverage.

## Production roadmap

1. Move SQL models and model tests to dbt.
2. Add orchestration with Airflow, Dagster, Prefect, or scheduled GitHub Actions.
3. Add alerting over `ops.pipeline_runs` and `ops.data_check_results`.
4. Store raw API responses for reproducible reruns.
5. Promote manual geocode overrides into a governed city dimension.
6. Add privacy retention rules for rejected raw payloads.
7. Add row-count anomaly detection and freshness SLAs.
