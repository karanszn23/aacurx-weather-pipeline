import pytest
import pandas as pd
from datetime import date, timedelta
from extract_weather import _build_fetch_windows, _to_date


# ---------------------------------------------------------------------------
# _to_date
# ---------------------------------------------------------------------------

def test_to_date_from_string():
    result = _to_date("2023-06-15")
    assert result == date(2023, 6, 15)


def test_to_date_from_timestamp():
    result = _to_date(pd.Timestamp("2023-06-15"))
    assert result == date(2023, 6, 15)


def test_to_date_none_returns_none():
    assert _to_date(None) is None


def test_to_date_nat_returns_none():
    assert _to_date(pd.NaT) is None


# ---------------------------------------------------------------------------
# _build_fetch_windows
# ---------------------------------------------------------------------------

JAN_1 = date(2023, 1, 1)
JUN_30 = date(2023, 6, 30)
DEC_31 = date(2023, 12, 31)
JUL_1 = date(2023, 7, 1)
MAR_31 = date(2023, 3, 31)


def test_build_fetch_windows_no_existing_data():
    windows = _build_fetch_windows(None, None, JAN_1, DEC_31)
    assert windows == [(JAN_1, DEC_31)]


def test_build_fetch_windows_already_fully_covered():
    # existing covers the full range — no fetch needed
    windows = _build_fetch_windows(JAN_1, DEC_31, JAN_1, DEC_31)
    assert windows == []


def test_build_fetch_windows_needs_tail_extension():
    # we have Jan–Jun, need Jan–Dec → only fetch Jul–Dec
    windows = _build_fetch_windows(JAN_1, JUN_30, JAN_1, DEC_31)
    assert windows == [(JUL_1, DEC_31)]


def test_build_fetch_windows_needs_head_extension():
    # we have Jul–Dec, target is Jan–Dec → only fetch Jan–Jun
    windows = _build_fetch_windows(JUL_1, DEC_31, JAN_1, DEC_31)
    assert windows == [(JAN_1, JUN_30)]


def test_build_fetch_windows_needs_both_ends():
    # we have Apr–Sep, target is Jan–Dec → fetch Jan–Mar and Oct–Dec
    apr_1 = date(2023, 4, 1)
    sep_30 = date(2023, 9, 30)
    oct_1 = date(2023, 10, 1)
    windows = _build_fetch_windows(apr_1, sep_30, JAN_1, DEC_31)
    assert len(windows) == 2
    assert windows[0] == (JAN_1, MAR_31)
    assert windows[1] == (oct_1, DEC_31)


def test_build_fetch_windows_target_inside_existing():
    # target is fully within existing — no windows
    windows = _build_fetch_windows(JAN_1, DEC_31, date(2023, 3, 1), date(2023, 9, 30))
    assert windows == []


def test_build_fetch_windows_adjacent_dates_produce_no_overlap():
    # existing ends Jun 30, target ends Dec 31 → window starts Jul 1 (no off-by-one)
    windows = _build_fetch_windows(JAN_1, JUN_30, JAN_1, DEC_31)
    start, end = windows[0]
    assert start == JUL_1
    assert (start - JUN_30).days == 1


def test_build_fetch_windows_single_day_gap():
    # existing is Jan 1 – Jan 1, target is Jan 1 – Jan 3 → fetch Jan 2 – Jan 3
    jan_2 = date(2023, 1, 2)
    jan_3 = date(2023, 1, 3)
    windows = _build_fetch_windows(JAN_1, JAN_1, JAN_1, jan_3)
    assert windows == [(jan_2, jan_3)]
