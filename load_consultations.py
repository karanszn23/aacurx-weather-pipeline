import json
import logging
import os
import pandas as pd
import duckdb
from config import DATA_DIR, DB_PATH, TIMEZONE

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = {"id", "city", "timestamp", "request_type"}
ALLOWED_REQUEST_TYPES = {"Medical", "Admin"}
INSERT_BATCH_SIZE = 50_000


def _safe_payload(raw):
    try:
        return json.dumps(raw, ensure_ascii=False)
    except TypeError:
        return str(raw)


def _validate_record(raw, source_file):
    if not isinstance(raw, dict):
        return None, {
            "source_file": source_file,
            "reason": "row is not a JSON object",
            "raw_payload": _safe_payload(raw),
        }

    missing = REQUIRED_FIELDS - set(raw.keys())
    if missing:
        return None, {
            "source_file": source_file,
            "reason": f"missing required fields: {sorted(missing)}",
            "raw_payload": _safe_payload(raw),
        }

    try:
        consultation_id = int(raw["id"])
    except (TypeError, ValueError):
        return None, {
            "source_file": source_file,
            "reason": "id is not an integer",
            "raw_payload": _safe_payload(raw),
        }

    city = str(raw["city"]).strip()
    if not city:
        return None, {
            "source_file": source_file,
            "reason": "city is empty",
            "raw_payload": _safe_payload(raw),
        }

    request_type = str(raw["request_type"]).strip()
    if request_type not in ALLOWED_REQUEST_TYPES:
        return None, {
            "source_file": source_file,
            "reason": f"request_type '{request_type}' is not allowed",
            "raw_payload": _safe_payload(raw),
        }

    # Parse timestamp. Source files contain naive timestamps representing Europe/London
    # wall-clock time. Localise explicitly so request_date derives the correct London date
    # (matters for BST dates where a naive CAST would give the wrong day near midnight).
    timestamp = pd.to_datetime(raw["timestamp"], errors="coerce")
    if pd.isna(timestamp):
        return None, {
            "source_file": source_file,
            "reason": "timestamp is invalid",
            "raw_payload": _safe_payload(raw),
        }
    try:
        timestamp_local = timestamp.tz_localize(TIMEZONE, ambiguous="NaT", nonexistent="NaT")
    except Exception:
        timestamp_local = pd.NaT
    if pd.isna(timestamp_local):
        return None, {
            "source_file": source_file,
            "reason": "timestamp is ambiguous or non-existent in Europe/London (DST boundary)",
            "raw_payload": _safe_payload(raw),
        }
    request_date = timestamp_local.date()

    return {
        "id": consultation_id,
        "city": city,
        "timestamp": timestamp,        # stored as naive London local time
        "request_date": request_date,  # derived from London-aware timestamp
        "request_type": request_type,
        "_source_file": source_file,
    }, None


def _iter_source_files(data_dir):
    for date_folder in sorted(os.listdir(data_dir)):
        folder_path = os.path.join(data_dir, date_folder)
        if not os.path.isdir(folder_path):
            continue
        for filename in sorted(os.listdir(folder_path)):
            if not filename.endswith(".json"):
                continue
            source_file = f"{date_folder}/{filename}"
            yield source_file, os.path.join(folder_path, filename)


def _ensure_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.consultations (
            id BIGINT PRIMARY KEY,
            city VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            request_date DATE NOT NULL,
            request_type VARCHAR NOT NULL,
            _source_file VARCHAR,
            loaded_at TIMESTAMP
        )
        """
    )
    con.execute("ALTER TABLE raw.consultations ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.consultation_rejections (
            source_file VARCHAR,
            reason VARCHAR,
            raw_payload VARCHAR,
            rejected_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.consultation_ingested_files (
            source_file VARCHAR,
            raw_row_count INTEGER,
            valid_row_count INTEGER,
            rejected_row_count INTEGER,
            processed_at TIMESTAMP
        )
        """
    )


def _read_new_records(data_dir, processed_files):
    valid_records = []
    rejected_records = []
    file_audit_rows = []

    for source_file, filepath in _iter_source_files(data_dir):
        if source_file in processed_files:
            continue

        raw_row_count = 0
        valid_count = 0
        rejected_count = 0
        try:
            with open(filepath, encoding="utf-8") as f:
                rows = json.load(f)
            if not isinstance(rows, list):
                raise ValueError("file root must be a JSON list")
            raw_row_count = len(rows)
        except Exception as exc:
            rejected_count = 1
            logger.warning("Failed to parse %s: %s", source_file, exc)
            rejected_records.append(
                {
                    "source_file": source_file,
                    "reason": f"failed to parse file: {exc}",
                    "raw_payload": None,
                }
            )
            file_audit_rows.append(
                {
                    "source_file": source_file,
                    "raw_row_count": raw_row_count,
                    "valid_row_count": valid_count,
                    "rejected_row_count": rejected_count,
                }
            )
            continue

        for row in rows:
            normalized, rejection = _validate_record(row, source_file)
            if rejection:
                rejected_count += 1
                rejected_records.append(rejection)
            else:
                valid_count += 1
                valid_records.append(normalized)

        file_audit_rows.append(
            {
                "source_file": source_file,
                "raw_row_count": raw_row_count,
                "valid_row_count": valid_count,
                "rejected_row_count": rejected_count,
            }
        )

    return valid_records, rejected_records, file_audit_rows


def _insert_consultations(con, records):
    """Insert records in batches to bound memory usage on large datasets."""
    total_inserted = 0
    for start in range(0, len(records), INSERT_BATCH_SIZE):
        batch = records[start : start + INSERT_BATCH_SIZE]
        df = pd.DataFrame(batch)
        dupes_in_batch = df.duplicated(subset=["id"]).sum()
        if dupes_in_batch:
            logger.warning("Dropping %d duplicate IDs within batch", dupes_in_batch)
            df = df.drop_duplicates(subset=["id"], keep="first")

        con.register("incoming_consultations_df", df)
        con.execute(
            """
            INSERT INTO raw.consultations (
                id, city, timestamp, request_date, request_type, _source_file, loaded_at
            )
            SELECT
                i.id,
                i.city,
                i.timestamp,
                i.request_date,
                i.request_type,
                i._source_file,
                CURRENT_TIMESTAMP
            FROM incoming_consultations_df i
            LEFT JOIN raw.consultations c ON i.id = c.id
            WHERE c.id IS NULL
            """
        )
        total_inserted += len(df)
    return total_inserted


def run(full_refresh=False):
    con = duckdb.connect(DB_PATH)
    try:
        _ensure_tables(con)

        if full_refresh:
            logger.info("Full refresh: clearing consultation tables")
            con.execute("DELETE FROM raw.consultations")
            con.execute("DELETE FROM raw.consultation_rejections")
            con.execute("DELETE FROM raw.consultation_ingested_files")

        processed = set(
            con.execute("SELECT DISTINCT source_file FROM raw.consultation_ingested_files")
            .fetchdf()["source_file"]
            .dropna()
            .tolist()
        )

        logger.info("Reading consultation JSON files...")
        records, rejections, file_audit = _read_new_records(DATA_DIR, processed)
        logger.info(
            "%d new files discovered; %d valid rows, %d rejected",
            len(file_audit), len(records), len(rejections),
        )

        if rejections:
            rej_df = pd.DataFrame(rejections)
            rej_df["rejected_at"] = pd.Timestamp.utcnow().tz_localize(None)
            con.register("consultation_rejections_df", rej_df)
            con.execute(
                """
                INSERT INTO raw.consultation_rejections
                SELECT source_file, reason, raw_payload, rejected_at
                FROM consultation_rejections_df
                """
            )

        if file_audit:
            audit_df = pd.DataFrame(file_audit)
            audit_df["processed_at"] = pd.Timestamp.utcnow().tz_localize(None)
            con.register("consultation_file_audit_df", audit_df)
            con.execute(
                """
                INSERT INTO raw.consultation_ingested_files
                SELECT source_file, raw_row_count, valid_row_count, rejected_row_count, processed_at
                FROM consultation_file_audit_df
                """
            )

        if not records:
            total = con.execute("SELECT COUNT(*) FROM raw.consultations").fetchone()[0]
            logger.info("No new valid records. Existing table has %d rows.", total)
            return

        before_count = con.execute("SELECT COUNT(*) FROM raw.consultations").fetchone()[0]
        _insert_consultations(con, records)
        after_count = con.execute("SELECT COUNT(*) FROM raw.consultations").fetchone()[0]
        inserted = after_count - before_count
        existing_dupes = len(records) - inserted

        stats = con.execute(
            """
            SELECT
                MIN(request_date) AS min_date,
                MAX(request_date) AS max_date,
                COUNT(DISTINCT city) AS city_count
            FROM raw.consultations
            """
        ).fetchdf()
        request_type_counts = con.execute(
            """
            SELECT request_type, COUNT(*) AS c
            FROM raw.consultations
            GROUP BY request_type
            ORDER BY request_type
            """
        ).fetchdf()
        type_summary = dict(zip(request_type_counts["request_type"], request_type_counts["c"]))

        logger.info("Inserted %d new rows into raw.consultations", inserted)
        logger.info("Skipped %d IDs already present in warehouse", existing_dupes)
        logger.info(
            "Date range: %s to %s | Cities: %d | Types: %s",
            stats["min_date"].iloc[0],
            stats["max_date"].iloc[0],
            stats["city_count"].iloc[0],
            type_summary,
        )
        logger.info("Total rows in raw.consultations: %d", after_count)
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
