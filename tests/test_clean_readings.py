"""Cleaning job (US-30): clock normalization (T-30.2)."""

from datetime import datetime

import pytest

from jobs.clean_readings import normalize_clock
from jobs.readings import READING_COLUMNS


def frame(spark, rows):
    """rows: (meter_id, original_ts, energy_text, seq)"""
    return spark.createDataFrame([(*r, "f3") for r in rows], "meter_id string, original_ts timestamp_ntz, "
                                 "energy_text string, seq long, source string").select(READING_COLUMNS)


@pytest.mark.parametrize("minute, expected", [
    (datetime(2013, 3, 4, 10, 33), datetime(2013, 3, 4, 10, 30)),
    (datetime(2013, 3, 4, 10, 35), datetime(2013, 3, 4, 10, 30)),   # exactly 5 minutes: still valid
    (datetime(2013, 3, 4, 10, 27), datetime(2013, 3, 4, 10, 30)),   # early clocks round up
    (datetime(2013, 3, 4, 10, 25), datetime(2013, 3, 4, 10, 30)),
    (datetime(2013, 3, 4, 10, 0), datetime(2013, 3, 4, 10, 0)),     # already on the interval
    (datetime(2013, 3, 4, 23, 57), datetime(2013, 3, 5, 0, 0)),     # crosses midnight
    (datetime(2013, 3, 4, 10, 41), None),                           # CA-01: 11 minutes away
    (datetime(2013, 3, 4, 10, 36), None),
    (datetime(2013, 3, 4, 10, 24), None),
])
def test_clock_is_normalized_within_5_minutes_or_flagged(spark, minute, expected):
    row = normalize_clock(frame(spark, [("MAC000002", minute, "0.2", 1)])).first()
    assert row.interval_start == expected
    assert row.clock_ok is (expected is not None)


def test_seconds_count_toward_the_5_minutes(spark):
    rows = normalize_clock(frame(spark, [
        ("M1", datetime(2013, 3, 4, 10, 5, 0), "0.2", 1),
        ("M2", datetime(2013, 3, 4, 10, 5, 1), "0.2", 1),
    ])).orderBy("meter_id").collect()
    assert [r.clock_ok for r in rows] == [True, False]


def test_missing_timestamp_is_an_invalid_clock(spark):
    row = normalize_clock(frame(spark, [("M1", None, "0.2", 1)])).first()
    assert row.interval_start is None and row.clock_ok is False
