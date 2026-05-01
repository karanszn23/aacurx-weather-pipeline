import logging
import os
import time
import pandas as pd
import duckdb
from config import (
    GEOCODE_API,
    DB_PATH,
    GEOCODE_COUNTRY_CODE,
    CITY_GEOCODE_OVERRIDES_PATH,
    GEOCODE_REQUEST_DELAY_SECONDS,
)
from http_utils import request_json_with_retries

logger = logging.getLogger(__name__)

UK_PRIORITY_CODES = {"GB", "GG", "JE", "IM"}
OVERRIDE_COLUMNS = {
    "city",
    "latitude",
    "longitude",
    "geocode_name",
    "admin1",
    "country",
    "country_code",
    "population",
}


def _ensure_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.city_coordinates (
            city VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            geocode_name VARCHAR,
            admin1 VARCHAR,
            country VARCHAR,
            country_code VARCHAR,
            population BIGINT,
            updated_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.city_geocode_failures (
            city VARCHAR,
            reason VARCHAR,
            failed_at TIMESTAMP
        )
        """
    )


def _hit_rank(hit):
    country_code = (hit.get("country_code") or "").upper()
    requested_country_rank = 0 if country_code == GEOCODE_COUNTRY_CODE.upper() else 1
    uk_rank = 0 if country_code in UK_PRIORITY_CODES else 1
    population_rank = -(hit.get("population") or 0)
    return requested_country_rank, uk_rank, population_rank


def _select_best_hit(hits):
    if not hits:
        return None
    return sorted(hits, key=_hit_rank)[0]


def _load_geocode_overrides(path):
    if not path or not os.path.exists(path):
        return {}

    df = pd.read_csv(path)
    missing = {"city", "latitude", "longitude"} - set(df.columns)
    if missing:
        raise ValueError(
            f"Geocode override file {path} missing required columns: {sorted(missing)}"
        )

    for column in OVERRIDE_COLUMNS - set(df.columns):
        df[column] = None

    overrides = {}
    for _, row in df.iterrows():
        city = str(row["city"]).strip()
        if not city:
            continue
        overrides[city] = {
            "city": city,
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "geocode_name": row["geocode_name"] if pd.notna(row["geocode_name"]) else city,
            "admin1": row["admin1"] if pd.notna(row["admin1"]) else None,
            "country": row["country"] if pd.notna(row["country"]) else None,
            "country_code": row["country_code"] if pd.notna(row["country_code"]) else None,
            "population": int(row["population"]) if pd.notna(row["population"]) else None,
        }
    return overrides


def geocode(city_name):
    params = {"name": city_name, "count": 5, "language": "en"}
    if GEOCODE_COUNTRY_CODE:
        params["countryCode"] = GEOCODE_COUNTRY_CODE

    payload = request_json_with_retries(GEOCODE_API, params=params)
    best = _select_best_hit(payload.get("results", []))
    if not best and GEOCODE_COUNTRY_CODE:
        fallback_payload = request_json_with_retries(
            GEOCODE_API, params={"name": city_name, "count": 5, "language": "en"}
        )
        best = _select_best_hit(fallback_payload.get("results", []))

    if not best:
        return None

    return {
        "city": city_name,
        "latitude": best["latitude"],
        "longitude": best["longitude"],
        "geocode_name": best.get("name"),
        "admin1": best.get("admin1"),
        "country": best.get("country"),
        "country_code": best.get("country_code"),
        "population": best.get("population"),
    }


def run(full_refresh=False):
    con = duckdb.connect(DB_PATH)
    try:
        _ensure_tables(con)

        if full_refresh:
            logger.info("Full refresh: clearing geocode cache tables")
            con.execute("DELETE FROM raw.city_coordinates")
            con.execute("DELETE FROM raw.city_geocode_failures")

        cities = (
            con.execute(
                "SELECT DISTINCT city FROM raw.consultations WHERE city IS NOT NULL ORDER BY city"
            )
            .fetchdf()["city"]
            .tolist()
        )
        cached = set(
            con.execute("SELECT DISTINCT city FROM raw.city_coordinates")
            .fetchdf()["city"]
            .dropna()
            .tolist()
        )
        remaining = [city for city in cities if city not in cached]
        overrides = _load_geocode_overrides(CITY_GEOCODE_OVERRIDES_PATH)

        logger.info(
            "Geocode cache: %d/%d cities. Resolving %d new cities (%d overrides available)...",
            len(cached), len(cities), len(remaining), len(overrides),
        )
        results, failed = [], []

        for i, city in enumerate(remaining):
            if i > 0 and i % 100 == 0:
                logger.info("Progress: %d/%d cities (%d failed)", i, len(remaining), len(failed))
            try:
                coords = overrides.get(city) or geocode(city)
                if coords:
                    results.append(coords)
                else:
                    logger.warning("No geocode results for: %s", city)
                    failed.append({"city": city, "reason": "no geocode results returned"})
            except Exception as exc:
                logger.warning("Geocode failed for %s: %s", city, exc)
                failed.append({"city": city, "reason": str(exc)})
            time.sleep(GEOCODE_REQUEST_DELAY_SECONDS)

        logger.info("Geocoding complete: %d succeeded, %d failed", len(results), len(failed))
        if failed:
            logger.warning("Failed cities (first 5): %s", [item["city"] for item in failed[:5]])

        if results:
            df = pd.DataFrame(results)
            df["updated_at"] = pd.Timestamp.utcnow().tz_localize(None)
            con.register("city_coords_df", df)
            con.execute(
                """
                INSERT INTO raw.city_coordinates
                SELECT
                    city, latitude, longitude, geocode_name,
                    admin1, country, country_code, population, updated_at
                FROM city_coords_df
                """
            )
            logger.info("Saved %d new rows to raw.city_coordinates", len(results))

        if failed:
            failed_df = pd.DataFrame(failed)
            failed_df["failed_at"] = pd.Timestamp.utcnow().tz_localize(None)
            con.register("city_geocode_failures_df", failed_df)
            con.execute(
                """
                INSERT INTO raw.city_geocode_failures
                SELECT city, reason, failed_at
                FROM city_geocode_failures_df
                """
            )

        total = con.execute("SELECT COUNT(*) FROM raw.city_coordinates").fetchone()[0]
        logger.info("Total cached city coordinates: %d", total)
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
