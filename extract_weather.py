import logging
import time
import pandas as pd
import duckdb
from datetime import timedelta
from config import (
    WEATHER_API,
    DB_PATH,
    TIMEZONE,
    WEATHER_REQUEST_DELAY_SECONDS,
    POOR_WEATHER_PRECIP_MM,
    POOR_WEATHER_WIND_KMH,
)
from http_utils import request_json_with_retries

logger = logging.getLogger(__name__)

DAILY_VARS = [
    "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "precipitation_sum", "rain_sum", "windspeed_10m_max", "weathercode",
]

# WMO weather codes that count as "poor" weather
POOR_WEATHER_CODES = {
    51, 53, 55,        # drizzle
    61, 63, 65,        # rain
    66, 67,            # freezing rain
    71, 73, 75, 77,    # snow
    80, 81, 82,        # showers
    85, 86,            # snow showers
    95, 96, 99,        # thunderstorm
}


def _ensure_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.weather (
            date DATE,
            temp_max DOUBLE,
            temp_min DOUBLE,
            temp_mean DOUBLE,
            precipitation_mm DOUBLE,
            rain_mm DOUBLE,
            wind_max_kmh DOUBLE,
            weather_code INTEGER,
            city VARCHAR,
            is_poor_weather BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.weather_fetch_failures (
            city VARCHAR,
            start_date DATE,
            end_date DATE,
            reason VARCHAR,
            failed_at TIMESTAMP
        )
        """
    )


def _to_date(value):
    if value is None or pd.isna(value):
        return None
    return pd.to_datetime(value).date()


def _build_fetch_windows(existing_start, existing_end, target_start, target_end):
    if existing_start is None or existing_end is None:
        return [(target_start, target_end)]

    windows = []
    if target_start < existing_start:
        windows.append((target_start, existing_start - timedelta(days=1)))
    if target_end > existing_end:
        windows.append((existing_end + timedelta(days=1), target_end))
    return [(start, end) for start, end in windows if start <= end]


def fetch_weather(lat, lon, start_date, end_date):
    payload = request_json_with_retries(
        WEATHER_API,
        params={
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": ",".join(DAILY_VARS),
            "timezone": TIMEZONE,
        },
    )

    daily = payload.get("daily", {})
    if not daily or not daily.get("time"):
        return pd.DataFrame()
    return pd.DataFrame({
        "date": pd.to_datetime(daily["time"]).date,
        "temp_max": daily["temperature_2m_max"],
        "temp_min": daily["temperature_2m_min"],
        "temp_mean": daily["temperature_2m_mean"],
        "precipitation_mm": daily["precipitation_sum"],
        "rain_mm": daily["rain_sum"],
        "wind_max_kmh": daily["windspeed_10m_max"],
        "weather_code": daily["weathercode"],
    })


def run(full_refresh=False):
    con = duckdb.connect(DB_PATH)
    try:
        _ensure_tables(con)

        if full_refresh:
            logger.info("Full refresh: clearing weather tables")
            con.execute("DELETE FROM raw.weather")
            con.execute("DELETE FROM raw.weather_fetch_failures")

        coords = con.execute(
            """
            SELECT city, latitude, longitude
            FROM (
                SELECT
                    city,
                    latitude,
                    longitude,
                    ROW_NUMBER() OVER (
                        PARTITION BY city
                        ORDER BY updated_at DESC NULLS LAST
                    ) AS rn
                FROM raw.city_coordinates
            )
            WHERE rn = 1
            ORDER BY city
            """
        ).fetchdf()
        dates = con.execute(
            "SELECT MIN(request_date) AS s, MAX(request_date) AS e FROM raw.consultations"
        ).fetchdf()

        target_start = _to_date(dates["s"].iloc[0])
        target_end = _to_date(dates["e"].iloc[0])

        if target_start is None or target_end is None:
            logger.warning("No consultation dates found; skipping weather extraction.")
            return

        if coords.empty:
            logger.warning("No city coordinates found; run geocode_cities first.")
            return

        existing_ranges_df = con.execute(
            """
            SELECT city, MIN(CAST(date AS DATE)) AS min_date, MAX(CAST(date AS DATE)) AS max_date
            FROM raw.weather
            GROUP BY city
            """
        ).fetchdf()
        existing_ranges = {
            row["city"]: (_to_date(row["min_date"]), _to_date(row["max_date"]))
            for _, row in existing_ranges_df.iterrows()
        }

        logger.info(
            "Fetching weather incrementally for %d cities (%s to %s)...",
            len(coords), target_start, target_end,
        )
        chunks, failed, api_calls = [], [], 0

        for i, row in coords.iterrows():
            city = row["city"]
            existing_start, existing_end = existing_ranges.get(city, (None, None))
            windows = _build_fetch_windows(existing_start, existing_end, target_start, target_end)
            if not windows:
                continue

            if i > 0 and i % 50 == 0:
                logger.info(
                    "Progress: %d/%d cities (%d API calls)", i, len(coords), api_calls
                )

            for window_start, window_end in windows:
                try:
                    city_weather = fetch_weather(
                        row["latitude"], row["longitude"], window_start, window_end
                    )
                    api_calls += 1
                    if city_weather.empty:
                        logger.warning(
                            "Empty weather payload for %s (%s to %s)",
                            city, window_start, window_end,
                        )
                        failed.append(
                            {
                                "city": city,
                                "start_date": window_start,
                                "end_date": window_end,
                                "reason": "empty weather payload",
                            }
                        )
                    else:
                        city_weather["city"] = city
                        chunks.append(city_weather)
                except Exception as exc:
                    logger.warning("Weather fetch failed for %s: %s", city, exc)
                    failed.append(
                        {
                            "city": city,
                            "start_date": window_start,
                            "end_date": window_end,
                            "reason": str(exc),
                        }
                    )
                time.sleep(WEATHER_REQUEST_DELAY_SECONDS)

        if failed:
            failed_df = pd.DataFrame(failed)
            failed_df["failed_at"] = pd.Timestamp.utcnow().tz_localize(None)
            con.register("weather_failures_df", failed_df)
            con.execute(
                """
                INSERT INTO raw.weather_fetch_failures
                SELECT city, start_date, end_date, reason, failed_at
                FROM weather_failures_df
                """
            )

        if not chunks:
            logger.info("No new weather windows to fetch; weather cache already up to date.")
            return

        weather = pd.concat(chunks, ignore_index=True)
        weather = weather.drop_duplicates(subset=["city", "date"])
        weather["precipitation_mm"] = weather["precipitation_mm"].fillna(0)
        weather["rain_mm"] = weather["rain_mm"].fillna(0)
        weather["weather_code"] = pd.to_numeric(weather["weather_code"], errors="coerce")

        # Classification thresholds are configurable via POOR_WEATHER_PRECIP_MM /
        # POOR_WEATHER_WIND_KMH environment variables (see config.py).
        weather["is_poor_weather"] = (
            weather["weather_code"].isin(POOR_WEATHER_CODES)
            | (weather["precipitation_mm"] > POOR_WEATHER_PRECIP_MM)
            | (weather["wind_max_kmh"] > POOR_WEATHER_WIND_KMH)
        )

        before_count = con.execute("SELECT COUNT(*) FROM raw.weather").fetchone()[0]
        con.register("incoming_weather_df", weather)
        con.execute(
            """
            INSERT INTO raw.weather (
                date, temp_max, temp_min, temp_mean,
                precipitation_mm, rain_mm, wind_max_kmh,
                weather_code, city, is_poor_weather
            )
            SELECT
                CAST(w.date AS DATE),
                w.temp_max, w.temp_min, w.temp_mean,
                w.precipitation_mm, w.rain_mm, w.wind_max_kmh,
                CAST(w.weather_code AS INTEGER),
                w.city, w.is_poor_weather
            FROM incoming_weather_df w
            LEFT JOIN raw.weather existing
                ON existing.city = w.city
                AND CAST(existing.date AS DATE) = CAST(w.date AS DATE)
            WHERE existing.city IS NULL
            """
        )
        after_count = con.execute("SELECT COUNT(*) FROM raw.weather").fetchone()[0]
        inserted = after_count - before_count

        logger.info(
            "API calls: %d | Failed windows: %d | Inserted: %d rows",
            api_calls, len(failed), inserted,
        )
        logger.info(
            "Poor weather days in batch: %d / %d",
            int(weather["is_poor_weather"].sum()), len(weather),
        )
        logger.info("Total rows in raw.weather: %d", after_count)
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
