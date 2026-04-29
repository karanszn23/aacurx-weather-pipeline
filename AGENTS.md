# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Repository purpose
This repo builds a local DuckDB warehouse that joins GP online consultation requests with historical weather, producing `marts.fct_daily_consultation_weather` for analysis.

## Environment and dependencies
- Python: `>=3.12` (from `pyproject.toml`)
- Install deps:
  - `pip install -r requirements.txt`
- Main libraries: DuckDB, pandas, requests

## Core development commands
- Run full pipeline (extract + transform + checks):
  - `python run_pipeline.py`
- Run only data quality checks against an existing `warehouse.duckdb`:
  - `python run_pipeline.py --check`
- Run individual ingestion/enrichment stages:
  - `python load_consultations.py`
  - `python geocode_cities.py`
  - `python extract_weather.py`
- Inspect mart output quickly:
  - `duckdb warehouse.duckdb -c "SELECT * FROM marts.fct_daily_consultation_weather LIMIT 20"`

## Tests, linting, and build status
- There is no configured pytest suite, linter, or separate build system in this repo right now.
- The closest equivalent to test execution is:
  - `python run_pipeline.py --check` (runs the `DATA_CHECKS` queries in `run_pipeline.py`)
- If you need to validate a single check, run the corresponding SQL directly via DuckDB against `warehouse.duckdb`.

## High-level architecture
Pipeline architecture is script-orchestrated ELT with DuckDB as the system of record:

1. **Raw load (`load_consultations.py`)**
   - Reads partitioned JSON files under `data/<date>/<hour>.json`.
   - Adds `_source_file` lineage, parses timestamps, deduplicates by `id`.
   - Writes `raw.consultations` in DuckDB.

2. **Reference enrichment (`geocode_cities.py`)**
   - Reads distinct cities from `raw.consultations`.
   - Calls Open-Meteo geocoding API (`GEOCODE_API`).
   - Writes `raw.city_coordinates`.

3. **Weather extraction (`extract_weather.py`)**
   - Reads city coordinates and min/max consultation dates from DuckDB.
   - Calls Open-Meteo historical archive API (`WEATHER_API`) for daily metrics.
   - Derives `is_poor_weather` from WMO codes, precipitation, and wind thresholds.
   - Writes `raw.weather`.

4. **SQL transforms (`sql/*.sql`, executed by `run_pipeline.py`)**
   - `sql/stg_consultations.sql` -> `staging.stg_consultations`
   - `sql/stg_weather.sql` -> `staging.stg_weather`
   - `sql/fct_daily_consultation_weather.sql` -> `marts.fct_daily_consultation_weather`
   - Final model is one row per city/day, with consultation aggregates LEFT JOINed to weather.

5. **Data quality checks (`run_pipeline.py`)**
   - Validates deduplication assumptions, category constraints, metric consistency, and non-negative counts.
   - Reports weather coverage (% of mart rows with weather).

## Important implementation details
- `config.py` is the shared configuration point for `DB_PATH`, `DATA_DIR`, and API endpoints.
- Stage ordering matters: `raw.consultations` must exist before geocoding; geocoded coordinates must exist before weather extraction.
- `run_pipeline.py` is the canonical orchestrator and should be kept aligned with any stage/model additions.
- Network/API behavior is intentionally conservative (timeouts, retries for weather rate limits, sleeps between requests), so full runs are expected to be slow (~20 minutes per README).
