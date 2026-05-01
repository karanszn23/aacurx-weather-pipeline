# Weather vs Online Consultations

Local ELT pipeline that joins GP online consultation requests with historical weather data. The output is `marts.fct_daily_consultation_weather`, a city/day table a data scientist can use to test whether medical request volume changes on poor-weather days.

## Why this project exists

The business question is:

> Do GP online medical requests increase on days with poor weather?

The pipeline turns hourly consultation JSON files into an analytical DuckDB warehouse:

1. Load and validate raw consultation events.
2. Geocode consultation cities to latitude/longitude.
3. Fetch historical daily weather for each geocoded city.
4. Build staging tables and a final mart at one row per city per day.
5. Run data-quality checks and record pipeline metadata.

## Setup

Python 3.12 or newer is expected.

```bash
pip install -r requirements.txt
```

For local development and tests, install the dev dependencies from `pyproject.toml` too:

```bash
pip install -e ".[dev]"
```

On this machine there may already be a `.venv`; if so, use:

```powershell
.\.venv\Scripts\python.exe -m pytest tests -q
```

## How to run

Run the full pipeline:

```bash
python run_pipeline.py
```

This can take around 20 minutes because the weather API is called once per geocoded city/date window. The run creates or updates `warehouse.duckdb`.

Query the mart:

```bash
duckdb warehouse.duckdb -c "SELECT * FROM marts.fct_daily_consultation_weather LIMIT 20"
```

Run only the data-quality checks against an existing complete warehouse:

```bash
python run_pipeline.py --check
```

Useful options:

```bash
python run_pipeline.py --full-refresh
python run_pipeline.py --skip-checks
```

You can also run stages separately:

```bash
python load_consultations.py
python geocode_cities.py
python extract_weather.py
```

## Configuration

All environment variables are optional and have defaults in `config.py`. Copy `.env.example` to `.env` if you want to override them.

Important settings:

- `DB_PATH`: DuckDB warehouse path, default `warehouse.duckdb`.
- `DATA_DIR`: input JSON directory, default `data`.
- `PIPELINE_TIMEZONE`: source timestamp interpretation and weather timezone, default `Europe/London`.
- `GEOCODE_COUNTRY_CODE`: preferred geocoding country, default `GB`.
- `CITY_GEOCODE_OVERRIDES_PATH`: optional CSV of curated city coordinate overrides, default `city_geocode_overrides.csv`.
- `HTTP_MAX_RETRIES`, `HTTP_BACKOFF_BASE_SECONDS`, `HTTP_TIMEOUT_SECONDS`: API resilience settings.
- `WEATHER_COVERAGE_ALERT_THRESHOLD`: minimum expected mart weather coverage percentage.
- `POOR_WEATHER_PRECIP_MM`, `POOR_WEATHER_WIND_KMH`: configurable poor-weather thresholds.

Manual geocode overrides can be supplied with the columns shown in `city_geocode_overrides.csv.example`. Overrides are useful for ambiguous city names where the API result is known to be wrong.

## Architecture

The project uses script-orchestrated ELT with DuckDB as the local warehouse.

### Raw load

`load_consultations.py` reads partitioned files under `data/<date>/<hour>.json`.

It:

- validates required fields: `id`, `city`, `timestamp`, `request_type`;
- rejects malformed records into `raw.consultation_rejections`;
- tracks processed files in `raw.consultation_ingested_files`;
- deduplicates by consultation `id`;
- stores accepted rows in `raw.consultations`;
- derives `request_date` using the configured London timezone.

### City geocoding

`geocode_cities.py` reads distinct cities from `raw.consultations` and calls the Open-Meteo geocoding API.

It:

- prefers GB results, then UK crown dependencies, then larger population matches;
- caches successful lookups in `raw.city_coordinates`;
- records failed lookups in `raw.city_geocode_failures`;
- avoids re-geocoding cities already in the cache unless `--full-refresh` is used.

### Weather extraction

`extract_weather.py` reads city coordinates and the consultation date range, then calls the Open-Meteo historical archive API.

It:

- fetches daily temperature, precipitation, rain, wind, and WMO weather code;
- fetches only missing head/tail date windows for each city;
- stores failures in `raw.weather_fetch_failures`;
- writes daily rows to `raw.weather`;
- derives `is_poor_weather` from WMO codes, precipitation, and wind thresholds.

### SQL models

The SQL models build the analytical layer:

- `sql/stg_consultations.sql`: casts and lightly cleans consultation rows.
- `sql/stg_weather.sql`: casts and renames weather fields.
- `sql/fct_daily_consultation_weather.sql`: aggregates consultations by city/day and left joins weather.

The final mart grain is one row per `city` and `request_date`.

## Data quality and observability

`run_pipeline.py` runs checks after the models are built.

Checks include:

- no duplicate consultation IDs;
- allowed request types only;
- no null critical consultation fields;
- one weather row per city/date;
- one mart row per city/date;
- medical plus admin counts equal total requests;
- medical percentage is between 0 and 100;
- request counts are non-negative;
- mart date range matches raw consultation date range;
- weather date range covers the consultation date range;
- weather coverage is above the configured threshold.

If a warehouse is incomplete, `python run_pipeline.py --check` now reports the missing required tables clearly instead of surfacing raw DuckDB catalog errors.

Full runs also write operational metadata:

- `ops.pipeline_runs`: run id, status, timings, row counts, and errors.
- `ops.data_check_results`: pass/fail result for each data-quality check.

## Tests and CI

Run tests:

```bash
pytest tests -q
```

The suite covers:

- consultation validation and source-file iteration;
- geocode result ranking;
- weather fetch-window calculation;
- HTTP retry behavior;
- a mocked end-to-end pipeline integration test using a temporary DuckDB database.

GitHub Actions runs the test suite on push and pull request via `.github/workflows/ci.yml`.

## Analysis examples

`analysis/weather_vs_consultations.sql` contains example queries for:

- comparing request volume on poor-weather vs non-poor-weather days;
- splitting that comparison by weekday/weekend;
- finding cities with the strongest observed difference;
- inspecting weather coverage by city.

These queries are starting points for exploration. They show association, not causation.

## Design choices

DuckDB keeps the project easy to run locally with no cloud credentials, while still giving SQL-first analytical modelling.

Weather is daily rather than hourly because the analytical hypothesis is about day-level poor weather. Hourly weather would add complexity and noise unless the question changed.

The mart uses a `LEFT JOIN` from consultations to weather so missing weather does not drop consultation activity. Weather coverage is measured separately by the checks.

The poor-weather flag is deliberately transparent and configurable. Raw weather metrics remain in the mart so a data scientist can test different definitions.

`day_of_week` and `day_name` are included because weekday/weekend patterns are likely confounders for consultation volume.

## Interview talking points

Be ready to explain:

- The final table grain: one row per city per request date.
- Why DuckDB is a good fit for local analytical ELT.
- How idempotency is handled through processed-file tracking, ID dedupe, cached geocodes, and incremental weather windows.
- Why bad input records are quarantined instead of crashing the load.
- Why missing weather is tolerated but measured.
- How unit tests and the mocked integration test prove the riskiest behavior.
- How you would evolve this into a production platform.

Additional docs:

- `docs/architecture.md`: system design, data flow, checks, and production roadmap.
- `docs/data_dictionary.md`: final mart and ops table definitions.
- `SECURITY.md`: privacy, third-party API, and data-handling notes.
- `INTERVIEW_NOTES.md`: concise interview pitch and Q&A.

## Production-readiness roadmap

Highest-value next steps:

- Move SQL models and tests into dbt or another model-management layer.
- Add scheduled orchestration with Airflow, Dagster, Prefect, or GitHub Actions.
- Add stronger city resolution: manual overrides, ambiguity review, confidence scoring, and a curated city dimension.
- Cache raw API responses so reruns are reproducible even if external APIs change.
- Emit structured JSON logs in production mode.
- Add freshness checks for latest consultation partition and latest weather date.
- Add alerting for failed runs, low weather coverage, high rejection rates, and API degradation.
- Pin dependencies more tightly for reproducible builds.
- Add type hints and static checks such as `ruff` and `mypy`.

## Known local-state note

The checked-in/local `warehouse.duckdb` may be partially built depending on the last run. If `--check` reports missing `raw.weather` or mart tables, run the full pipeline first or use a fresh `DB_PATH` for a complete rebuild.
