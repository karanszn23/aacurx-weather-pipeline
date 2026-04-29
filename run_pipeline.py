import argparse
import logging
import sys
import duckdb
import load_consultations
import geocode_cities
import extract_weather
from config import DB_PATH, WEATHER_COVERAGE_ALERT_THRESHOLD

logger = logging.getLogger(__name__)

SQL_MODELS = [
    "sql/stg_consultations.sql",
    "sql/stg_weather.sql",
    "sql/fct_daily_consultation_weather.sql",
]

# Each check is a query that should return 0 rows when healthy.
DATA_CHECKS = [
    (
        "no duplicate consultation IDs",
        "SELECT id, COUNT(*) c FROM raw.consultations GROUP BY id HAVING c > 1",
    ),
    (
        "request_type is Medical or Admin only",
        "SELECT DISTINCT request_type FROM raw.consultations WHERE request_type NOT IN ('Medical','Admin')",
    ),
    (
        "consultations have no null critical fields",
        "SELECT * FROM raw.consultations WHERE id IS NULL OR city IS NULL OR request_date IS NULL OR request_type IS NULL",
    ),
    (
        "weather has one row per city/date",
        "SELECT city, date, COUNT(*) c FROM raw.weather GROUP BY city, date HAVING c > 1",
    ),
    (
        "medical + admin = total in mart",
        "SELECT * FROM marts.fct_daily_consultation_weather WHERE medical_requests + admin_requests != total_requests",
    ),
    (
        "medical_pct between 0 and 100",
        "SELECT * FROM marts.fct_daily_consultation_weather WHERE medical_pct < 0 OR medical_pct > 100",
    ),
    (
        "no negative request counts",
        "SELECT * FROM marts.fct_daily_consultation_weather WHERE total_requests < 0 OR medical_requests < 0",
    ),
]


def run_sql_models():
    con = duckdb.connect(DB_PATH)
    try:
        for path in SQL_MODELS:
            logger.info("Running %s", path)
            with open(path, encoding="utf-8") as f:
                con.execute(f.read())
    finally:
        con.close()


def run_checks():
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
    except Exception as exc:
        logger.error("Unable to open warehouse for checks: %s", exc)
        return False

    try:
        passed, failed = 0, 0
        logger.info("Running %d data quality checks...", len(DATA_CHECKS))
        for name, sql in DATA_CHECKS:
            try:
                rows = len(con.execute(sql).fetchdf())
                if rows == 0:
                    logger.info("  PASS  %s", name)
                    passed += 1
                else:
                    logger.error("  FAIL  %s (%d bad rows)", name, rows)
                    failed += 1
            except Exception as exc:
                logger.error("  FAIL  %s (query error: %s)", name, exc)
                failed += 1

        try:
            coverage = con.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(is_poor_weather) AS with_weather,
                    ROUND(COUNT(is_poor_weather) * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct
                FROM marts.fct_daily_consultation_weather
                """
            ).fetchdf()
            pct = float(coverage["pct"].iloc[0] or 0.0)
            with_weather = int(coverage["with_weather"].iloc[0] or 0)
            total = int(coverage["total"].iloc[0] or 0)

            logger.info("Weather coverage: %.1f%% (%d / %d rows)", pct, with_weather, total)
            if total > 0 and pct < WEATHER_COVERAGE_ALERT_THRESHOLD:
                logger.error(
                    "  FAIL  weather coverage below threshold (%.1f%% < %.1f%%)",
                    pct, WEATHER_COVERAGE_ALERT_THRESHOLD,
                )
                failed += 1
            else:
                logger.info(
                    "  PASS  weather coverage threshold (%.1f%%)",
                    WEATHER_COVERAGE_ALERT_THRESHOLD,
                )
                passed += 1
        except Exception as exc:
            logger.error("  FAIL  weather coverage check (query error: %s)", exc)
            failed += 1

        logger.info("%d passed, %d failed", passed, failed)
        return failed == 0
    finally:
        con.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Run consultation + weather data pipeline")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Run only data quality checks against an existing warehouse",
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Skip checks at the end of a full pipeline run",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Clear raw pipeline caches and reload data from scratch",
    )
    return parser.parse_args()


def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    _configure_logging()
    args = parse_args()

    if args.check:
        ok = run_checks()
        sys.exit(0 if ok else 1)

    logger.info("=" * 40)
    logger.info("STEP 1: Load consultations")
    logger.info("=" * 40)
    load_consultations.run(full_refresh=args.full_refresh)

    logger.info("=" * 40)
    logger.info("STEP 2: Geocode cities")
    logger.info("=" * 40)
    geocode_cities.run(full_refresh=args.full_refresh)

    logger.info("=" * 40)
    logger.info("STEP 3: Fetch weather")
    logger.info("=" * 40)
    extract_weather.run(full_refresh=args.full_refresh)

    logger.info("=" * 40)
    logger.info("STEP 4: Build models")
    logger.info("=" * 40)
    run_sql_models()

    logger.info("=" * 40)
    logger.info("STEP 5: Data checks")
    logger.info("=" * 40)
    if args.skip_checks:
        logger.info("Checks skipped (--skip-checks).")
    else:
        ok = run_checks()
        if not ok:
            sys.exit(1)

    logger.info("Done. Query the output:")
    logger.info(
        '  duckdb warehouse.duckdb -c "SELECT * FROM marts.fct_daily_consultation_weather LIMIT 20"'
    )


if __name__ == "__main__":
    main()
