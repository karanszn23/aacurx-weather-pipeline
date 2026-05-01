import json
from datetime import timedelta

import pandas as pd

import extract_weather
import geocode_cities
import load_consultations
import run_pipeline


def test_full_pipeline_with_mocked_api_enrichment(tmp_path, monkeypatch):
    db_path = tmp_path / "warehouse.duckdb"
    data_dir = tmp_path / "data"
    day_dir = data_dir / "2023-01-01"
    day_dir.mkdir(parents=True)
    (day_dir / "00.json").write_text(
        json.dumps(
            [
                {
                    "id": 1,
                    "city": "Leeds",
                    "timestamp": "2023-01-01T08:00:00",
                    "request_type": "Medical",
                },
                {
                    "id": 2,
                    "city": "Leeds",
                    "timestamp": "2023-01-01T09:00:00",
                    "request_type": "Admin",
                },
                {
                    "id": 3,
                    "city": "York",
                    "timestamp": "2023-01-01T10:00:00",
                    "request_type": "Medical",
                },
            ]
        ),
        encoding="utf-8",
    )

    for module in [load_consultations, geocode_cities, extract_weather, run_pipeline]:
        monkeypatch.setattr(module, "DB_PATH", str(db_path))
    monkeypatch.setattr(load_consultations, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(geocode_cities, "GEOCODE_REQUEST_DELAY_SECONDS", 0)
    monkeypatch.setattr(extract_weather, "WEATHER_REQUEST_DELAY_SECONDS", 0)

    def fake_geocode(city):
        return {
            "city": city,
            "latitude": 53.8,
            "longitude": -1.5,
            "geocode_name": city,
            "admin1": "England",
            "country": "United Kingdom",
            "country_code": "GB",
            "population": 100_000,
        }

    def fake_fetch_weather(lat, lon, start_date, end_date):
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return pd.DataFrame(
            {
                "date": dates,
                "temp_max": [8.0] * len(dates),
                "temp_min": [3.0] * len(dates),
                "temp_mean": [5.5] * len(dates),
                "precipitation_mm": [6.0] * len(dates),
                "rain_mm": [6.0] * len(dates),
                "wind_max_kmh": [20.0] * len(dates),
                "weather_code": [61] * len(dates),
            }
        )

    monkeypatch.setattr(geocode_cities, "geocode", fake_geocode)
    monkeypatch.setattr(extract_weather, "fetch_weather", fake_fetch_weather)

    load_consultations.run(full_refresh=True)
    geocode_cities.run(full_refresh=True)
    extract_weather.run(full_refresh=True)
    run_pipeline.run_sql_models()

    assert run_pipeline.run_checks()
