"""Checks used in the review of the cleaning job (US-30, T-30.6)."""

from datetime import datetime

from jobs.clean_readings import clean
from jobs.clean_report import as_markdown, report
from jobs.readings import READING_COLUMNS


def test_report_counts_flags_and_finds_no_duplicates_after_cleaning(spark):
    t = datetime(2013, 3, 4, 10, 0)
    raw = spark.createDataFrame([
        ("M1", t, "0.2", 1, "f3"), ("M1", t, "0.3", 2, "f3"),     # duplicate: one stays
        ("M2", t, "Null", 1, "f3"), ("M3", datetime(2013, 3, 4, 10, 41), "0.1", 1, "f3"),
    ], "meter_id string, original_ts timestamp_ntz, energy_text string, seq long, source string").select(READING_COLUMNS)
    r = report(clean(raw))
    assert r["rows"] == 3
    assert r["duplicates"] == 0
    assert r["flags"] == {("f3", "ok"): 1, ("f3", "null_value"): 1, ("f3", "invalid_clock"): 1}
    assert r["meters"] == 3 and r["dates"] == 1
    text = as_markdown(r)
    assert "| Duplicates (meter + interval) | 0 |" in text
    assert "| f3 | `ok` | 1 | 33.33% |" in text
