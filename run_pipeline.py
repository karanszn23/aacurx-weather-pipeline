import argparse
import logging
import sys
import traceback
import uuid
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
    (
        "mart has one row per city/date",
        "SELECT city, request_date, COUNT(*) c FROM marts.fct_daily_consultation_weather GROUP BY city, request_date HAVING c > 1",
    ),
    (
        "mart has no null grain fields",
        "SELECT * FROM marts.fct_daily_consultation_weather WHERE city IS NULL OR request_date IS NULL",
    ),
    (
        "mart date range matches raw consultations",
        """
        WITH raw_dates AS (
            SELECT MIN(request_date) AS min_date, MAX(request_date) AS max_date
            FROM raw.consultations
        ),
        mart_dates AS (
            SELECT MIN(request_date) AS min_date, MAX(request_date) AS max_date
            FROM marts.fct_daily_consultation_weather
        )
        SELECT *
        FROM raw_dates r, mart_dates m
        WHERE r.min_date != m.min_date OR r.max_date != m.max_date
        """,
    ),
    (
        "weather date range covers consultation date range",
        """
        WITH consultation_dates AS (
            SELECT MIN(request_date) AS min_date, MAX(request_date) AS max_date
            FROM raw.consultations
        ),
        weather_dates AS (
            SELECT MIN(CAST(date AS DATE)) AS min_date, MAX(CAST(date AS DATE)) AS max_date
            FROM raw.weather
        )
        SELECT *
        FROM consultation_dates c, weather_dates w
        WHERE w.min_date > c.min_date OR w.max_date < c.max_date
        """,
    ),
]

REQUIRED_CHECK_TABLES = [
    "raw.consultations",
    "raw.weather",
    "marts.fct_daily_consultation_weather",
]


def _ensure_ops_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
            run_id VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR,
            full_refresh BOOLEAN,
            skip_checks BOOLEAN,
            error_message VARCHAR,
            consultation_rows BIGINT,
            weather_rows BIGINT,
            mart_rows BIGINT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.data_check_results (
            run_id VARCHAR,
            check_name VARCHAR,
            status VARCHAR,
            bad_row_count BIGINT,
            error_message VARCHAR,
            checked_at TIMESTAMP
        )
        """
    )


def _table_exists(con, qualified_name):
    schema_name, table_name = qualified_name.split(".", 1)
    return con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()[0] > 0


def _count_if_exists(con, qualified_name):
    if not _table_exists(con, qualified_name):
        return None
    return con.execute(f"SELECT COUNT(*) FROM {qualified_name}").fetchone()[0]


def _start_pipeline_run(full_refresh, skip_checks):
    run_id = str(uuid.uuid4())
    con = duckdb.connect(DB_PATH)
    try:
        _ensure_ops_tables(con)
        con.execute(
            """
            INSERT INTO ops.pipeline_runs (
                run_id, started_at, status, full_refresh, skip_checks
            )
            VALUES (?, CURRENT_TIMESTAMP, 'running', ?, ?)
            """,
            [run_id, full_refresh, skip_checks],
        )
    finally:
        con.close()
    return run_id


def _finish_pipeline_run(run_id, status, error_message=None):
    if not run_id:
        return

    con = duckdb.connect(DB_PATH)
    try:
        _ensure_ops_tables(con)
        con.execute(
            """
            UPDATE ops.pipeline_runs
            SET
                finished_at = CURRENT_TIMESTAMP,
                status = ?,
                error_message = ?,
                consultation_rows = ?,
                weather_rows = ?,
                mart_rows = ?
            WHERE run_id = ?
            """,
            [
                status,
                error_message,
                _count_if_exists(con, "raw.consultations"),
                _count_if_exists(con, "raw.weather"),
                _count_if_exists(con, "marts.fct_daily_consultation_weather"),
                run_id,
            ],
        )
    finally:
        con.close()


def _record_check_result(con, run_id, check_name, status, bad_row_count=0, error_message=None):
    if not run_id:
        return
    _ensure_ops_tables(con)
    con.execute(
        """
        INSERT INTO ops.data_check_results (
            run_id, check_name, status, bad_row_count, error_message, checked_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [run_id, check_name, status, bad_row_count, error_message],
    )


def run_sql_models():
    con = duckdb.connect(DB_PATH)
    try:
        for path in SQL_MODELS:
            logger.info("Running %s", path)
            with open(path, encoding="utf-8") as f:
                con.execute(f.read())
    finally:
        con.close()


def run_checks(run_id=None):
    try:
        con = duckdb.connect(DB_PATH, read_only=run_id is None)
    except Exception as exc:
        logger.error("Unable to open warehouse for checks: %s", exc)
        return False

    try:
        passed, failed = 0, 0
        missing_tables = [
            table_name for table_name in REQUIRED_CHECK_TABLES
            if not _table_exists(con, table_name)
        ]
        if missing_tables:
            logger.error(
                "Pipeline is not fully built; missing required tables for checks: %s",
                ", ".join(missing_tables),
            )
            logger.error(
                "Run `python run_pipeline.py` to build the full warehouse, or inspect the failed stage logs."
            )
            for table_name in missing_tables:
                _record_check_result(
                    con,
                    run_id,
                    f"required table exists: {table_name}",
                    "fail",
                    1,
                    "missing required table",
                )
            return False

        logger.info("Running %d data quality checks...", len(DATA_CHECKS))
        for name, sql in DATA_CHECKS:
            try:
                rows = len(con.execute(sql).fetchdf())
                if rows == 0:
                    logger.info("  PASS  %s", name)
                    _record_check_result(con, run_id, name, "pass", 0)
                    passed += 1
                else:
                    logger.error("  FAIL  %s (%d bad rows)", name, rows)
                    _record_check_result(con, run_id, name, "fail", rows)
                    failed += 1
            except Exception as exc:
                logger.error("  FAIL  %s (query error: %s)", name, exc)
                _record_check_result(con, run_id, name, "fail", None, str(exc))
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
                _record_check_result(
                    con,
                    run_id,
                    "weather coverage threshold",
                    "fail",
                    total - with_weather,
                    f"{pct:.1f}% below {WEATHER_COVERAGE_ALERT_THRESHOLD:.1f}%",
                )
                failed += 1
            else:
                logger.info(
                    "  PASS  weather coverage threshold (%.1f%%)",
                    WEATHER_COVERAGE_ALERT_THRESHOLD,
                )
                _record_check_result(con, run_id, "weather coverage threshold", "pass", 0)
                passed += 1
        except Exception as exc:
            logger.error("  FAIL  weather coverage check (query error: %s)", exc)
            _record_check_result(con, run_id, "weather coverage threshold", "fail", None, str(exc))
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

    run_id = _start_pipeline_run(args.full_refresh, args.skip_checks)
    logger.info("Pipeline run_id: %s", run_id)
    try:
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
            ok = run_checks(run_id=run_id)
            if not ok:
                _finish_pipeline_run(run_id, "failed", "data quality checks failed")
                sys.exit(1)

        _finish_pipeline_run(run_id, "succeeded")
        logger.info("Done. Query the output:")
        logger.info(
            '  duckdb warehouse.duckdb -c "SELECT * FROM marts.fct_daily_consultation_weather LIMIT 20"'
        )
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        _finish_pipeline_run(run_id, "failed", traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
