# Weather vs Online Consultations

Pipeline that joins GP online consultation requests with historical weather data. The output is a table a data scientist can use to test whether medical requests increase on days with poor weather.

## How to run

```
pip install -r requirements.txt
python run_pipeline.py
```

This takes about 20 minutes (mostly waiting on the weather API). It creates a `warehouse.duckdb` file you can query:

```
duckdb warehouse.duckdb -c "SELECT * FROM marts.fct_daily_consultation_weather LIMIT 20"
```

You can also run each step separately if something fails:

```
python load_consultations.py
python geocode_cities.py
python extract_weather.py
```

And run data quality checks against an existing warehouse:

```
python run_pipeline.py --check
```

## What it does

The pipeline has four steps.

`load_consultations.py` reads the date/hour partitioned JSON files and loads them into DuckDB as `raw.consultations`. It adds a `_source_file` column for lineage and deduplicates on ID.

`geocode_cities.py` takes the 674 distinct cities from the consultation data and looks up lat/lon coordinates using Open-Meteo's geocoding API. Some small towns won't geocode — that's fine, we work with what we get.

`extract_weather.py` calls the Open-Meteo Historical Weather API for each geocoded city, pulling a full year of daily weather in one call per city. It fetches temperature, precipitation, wind speed, and WMO weather codes. It also flags "poor weather" days based on the weather code, precipitation above 5mm, or wind above 50 km/h.

The SQL files in `sql/` clean the data (staging layer) and build the final analytical table (marts layer). The mart joins consultation counts per city per day with weather data. It uses a LEFT JOIN so we don't lose consultation rows where weather is missing.

## Design choices

I used DuckDB so everything runs locally with no cloud credentials. The SQL is standard and would work in Snowflake with minimal changes.

Weather grain is daily, not hourly, because the hypothesis is about days with poor weather. Hourly would add noise without improving the analysis.

The "poor weather" flag is subjective, so I kept the raw metrics (temp, precipitation, wind, weather code) in the final table. The data scientist can define their own threshold.

I included `day_of_week` in the model because weekday vs weekend affects consultation volume regardless of weather. It's a confounder the DS should control for.

## Assumptions

Timestamps are treated as Europe/London timezone. The missing `01.json` on March 26 is the BST clock change (no 1am that day) — handled naturally since we just read whatever files exist. Cities that don't geocode are skipped but logged.

## What I'd add in production

Incremental loading (only process new date partitions). Retry with backoff on API failures. An orchestrator like Airflow. dbt for model management and testing. Monitoring for data freshness and weather coverage gaps.
