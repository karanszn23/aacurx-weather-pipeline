import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _get_env_int(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Environment variable {name}={value!r} is not a valid integer") from None


def _get_env_float(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"Environment variable {name}={value!r} is not a valid float") from None


DB_PATH = os.getenv("DB_PATH", "warehouse.duckdb")
DATA_DIR = os.getenv("DATA_DIR", "data")

WEATHER_API = os.getenv("WEATHER_API", "https://archive-api.open-meteo.com/v1/archive")
GEOCODE_API = os.getenv("GEOCODE_API", "https://geocoding-api.open-meteo.com/v1/search")
TIMEZONE = os.getenv("PIPELINE_TIMEZONE", "Europe/London")

GEOCODE_COUNTRY_CODE = os.getenv("GEOCODE_COUNTRY_CODE", "GB")
CITY_GEOCODE_OVERRIDES_PATH = os.getenv(
    "CITY_GEOCODE_OVERRIDES_PATH", "city_geocode_overrides.csv"
)
GEOCODE_REQUEST_DELAY_SECONDS = _get_env_float("GEOCODE_REQUEST_DELAY_SECONDS", 0.2)
WEATHER_REQUEST_DELAY_SECONDS = _get_env_float("WEATHER_REQUEST_DELAY_SECONDS", 1.0)

HTTP_MAX_RETRIES = _get_env_int("HTTP_MAX_RETRIES", 4)
HTTP_BACKOFF_BASE_SECONDS = _get_env_float("HTTP_BACKOFF_BASE_SECONDS", 2.0)
HTTP_BACKOFF_MAX_SECONDS = _get_env_float("HTTP_BACKOFF_MAX_SECONDS", 60.0)
HTTP_TIMEOUT_SECONDS = _get_env_int("HTTP_TIMEOUT_SECONDS", 30)

WEATHER_COVERAGE_ALERT_THRESHOLD = _get_env_float("WEATHER_COVERAGE_ALERT_THRESHOLD", 95.0)

# Poor weather classification thresholds — used in extract_weather.py
POOR_WEATHER_PRECIP_MM = _get_env_float("POOR_WEATHER_PRECIP_MM", 5.0)
POOR_WEATHER_WIND_KMH = _get_env_float("POOR_WEATHER_WIND_KMH", 50.0)
