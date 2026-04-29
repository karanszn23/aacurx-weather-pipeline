import json
import os
import tempfile
import pytest
from load_consultations import _validate_record, _safe_payload, _iter_source_files


# ---------------------------------------------------------------------------
# _safe_payload
# ---------------------------------------------------------------------------

def test_safe_payload_serialisable():
    assert _safe_payload({"key": "value"}) == '{"key": "value"}'


def test_safe_payload_unserializable_falls_back_to_str():
    class Unserializable:
        pass
    result = _safe_payload(Unserializable())
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# _validate_record — happy path
# ---------------------------------------------------------------------------

VALID_RAW = {
    "id": 9562,
    "city": "Scunthorpe",
    "timestamp": "2023-01-01T01:31:41",
    "request_type": "Medical",
}


def test_validate_record_valid_medical():
    record, rejection = _validate_record(VALID_RAW, "2023-01-01/01.json")
    assert rejection is None
    assert record["id"] == 9562
    assert record["city"] == "Scunthorpe"
    assert record["request_type"] == "Medical"
    assert record["request_date"].isoformat() == "2023-01-01"
    assert record["_source_file"] == "2023-01-01/01.json"


def test_validate_record_valid_admin():
    raw = {**VALID_RAW, "request_type": "Admin"}
    record, rejection = _validate_record(raw, "f.json")
    assert rejection is None
    assert record["request_type"] == "Admin"


def test_validate_record_string_id_coerced():
    raw = {**VALID_RAW, "id": "9562"}
    record, rejection = _validate_record(raw, "f.json")
    assert rejection is None
    assert record["id"] == 9562


def test_validate_record_trims_city_whitespace():
    raw = {**VALID_RAW, "city": "  Leeds  "}
    record, rejection = _validate_record(raw, "f.json")
    assert rejection is None
    assert record["city"] == "Leeds"


def test_validate_record_trims_request_type_whitespace():
    raw = {**VALID_RAW, "request_type": " Medical "}
    record, rejection = _validate_record(raw, "f.json")
    assert rejection is None
    assert record["request_type"] == "Medical"


def test_validate_record_bst_summer_date():
    # 2023-06-15 is in BST (UTC+1). Localising to Europe/London should give date 2023-06-15.
    raw = {**VALID_RAW, "timestamp": "2023-06-15T12:00:00"}
    record, rejection = _validate_record(raw, "f.json")
    assert rejection is None
    assert record["request_date"].isoformat() == "2023-06-15"


# ---------------------------------------------------------------------------
# _validate_record — rejections
# ---------------------------------------------------------------------------

def test_validate_record_not_a_dict():
    record, rejection = _validate_record("not a dict", "f.json")
    assert record is None
    assert "not a JSON object" in rejection["reason"]


def test_validate_record_missing_fields():
    raw = {"id": 1, "city": "London"}  # missing timestamp and request_type
    record, rejection = _validate_record(raw, "f.json")
    assert record is None
    assert "missing required fields" in rejection["reason"]
    assert "request_type" in rejection["reason"]
    assert "timestamp" in rejection["reason"]


def test_validate_record_non_integer_id():
    raw = {**VALID_RAW, "id": "not-a-number"}
    record, rejection = _validate_record(raw, "f.json")
    assert record is None
    assert "id is not an integer" in rejection["reason"]


def test_validate_record_none_id():
    raw = {**VALID_RAW, "id": None}
    record, rejection = _validate_record(raw, "f.json")
    assert record is None
    assert "id is not an integer" in rejection["reason"]


def test_validate_record_empty_city():
    raw = {**VALID_RAW, "city": "   "}
    record, rejection = _validate_record(raw, "f.json")
    assert record is None
    assert "city is empty" in rejection["reason"]


def test_validate_record_invalid_request_type():
    raw = {**VALID_RAW, "request_type": "Dental"}
    record, rejection = _validate_record(raw, "f.json")
    assert record is None
    assert "Dental" in rejection["reason"]


def test_validate_record_invalid_timestamp():
    raw = {**VALID_RAW, "timestamp": "not-a-date"}
    record, rejection = _validate_record(raw, "f.json")
    assert record is None
    assert "timestamp is invalid" in rejection["reason"]


def test_validate_record_null_timestamp():
    raw = {**VALID_RAW, "timestamp": None}
    record, rejection = _validate_record(raw, "f.json")
    assert record is None
    assert "timestamp is invalid" in rejection["reason"]


def test_validate_record_raw_payload_captured_on_rejection():
    raw = {**VALID_RAW, "id": "bad"}
    _, rejection = _validate_record(raw, "f.json")
    assert rejection["raw_payload"] is not None
    assert "bad" in rejection["raw_payload"]


# ---------------------------------------------------------------------------
# _iter_source_files
# ---------------------------------------------------------------------------

def test_iter_source_files_yields_json_only(tmp_path):
    date_dir = tmp_path / "2023-01-01"
    date_dir.mkdir()
    (date_dir / "01.json").write_text("[]")
    (date_dir / "README.txt").write_text("ignore me")

    results = list(_iter_source_files(str(tmp_path)))
    assert len(results) == 1
    source_file, filepath = results[0]
    assert source_file == "2023-01-01/01.json"
    assert os.path.exists(filepath)


def test_iter_source_files_sorted_order(tmp_path):
    for date in ["2023-01-03", "2023-01-01", "2023-01-02"]:
        d = tmp_path / date
        d.mkdir()
        (d / "00.json").write_text("[]")

    source_files = [sf for sf, _ in _iter_source_files(str(tmp_path))]
    assert source_files == [
        "2023-01-01/00.json",
        "2023-01-02/00.json",
        "2023-01-03/00.json",
    ]


def test_iter_source_files_skips_non_directories(tmp_path):
    (tmp_path / "stray_file.json").write_text("[]")
    date_dir = tmp_path / "2023-01-01"
    date_dir.mkdir()
    (date_dir / "00.json").write_text("[]")

    source_files = [sf for sf, _ in _iter_source_files(str(tmp_path))]
    assert source_files == ["2023-01-01/00.json"]
