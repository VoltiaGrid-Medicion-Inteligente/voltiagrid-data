"""Cleaning job (US-30): clock normalization (T-30.2), duplicates (T-30.3)."""

from datetime import datetime

import pytest

from jobs.clean_readings import deduplicate, normalize_clock
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


T = datetime(2013, 3, 4, 10, 30)


def dedup(spark, rows):
    return sorted(deduplicate(normalize_clock(frame(spark, rows))).collect(),
                  key=lambda r: (r.meter_id, str(r.interval_start), r.seq))


def test_ca_02_same_interval_keeps_the_highest_sequence(spark):
    rows = dedup(spark, [("MAC000002", T, "0.20", 1), ("MAC000002", T, "0.30", 2)])
    assert [(r.seq, r.energy_text) for r in rows] == [(2, "0.30")]


def test_readings_that_normalize_to_the_same_interval_are_duplicates(spark):
    rows = dedup(spark, [
        ("M1", datetime(2013, 3, 4, 10, 28), "0.40", 5),
        ("M1", datetime(2013, 3, 4, 10, 31), "0.10", 3),
    ])
    assert [(r.seq, r.interval_start) for r in rows] == [(5, T)]


def test_highest_sequence_wins_even_when_its_value_is_null(spark):
    rows = dedup(spark, [("M1", T, "0.30", 3), ("M1", T, None, 5)])
    assert [(r.seq, r.energy_text) for r in rows] == [(5, None)]


def test_exact_redelivery_leaves_one_row(spark):
    rows = dedup(spark, [("M1", T, "0.30", 7)] * 3)
    assert len(rows) == 1


def test_no_two_rows_share_meter_interval_and_sequence_after_a_resend(spark):
    resend = [("M%d" % m, datetime(2013, 3, 4, h, 0), "0.1", m * 100 + h) for m in range(5) for h in range(24)]
    rows = dedup(spark, resend + resend)
    keys = [(r.meter_id, r.interval_start, r.seq) for r in rows]
    assert len(keys) == len(set(keys)) == 5 * 24


def test_different_meters_or_intervals_are_not_merged(spark):
    rows = dedup(spark, [("M1", T, "0.1", 1), ("M2", T, "0.2", 1), ("M1", datetime(2013, 3, 4, 11, 0), "0.3", 1)])
    assert len(rows) == 3


def test_f1_ties_without_sequence_are_resolved_the_same_way_every_time(spark):
    rows = dedup(spark, [("M1", T, "0.10", 0), ("M1", T, "0.25", 0)])
    assert [r.energy_text for r in rows] == ["0.25"]


def test_invalid_clock_readings_are_kept_for_the_quality_report(spark):
    rows = dedup(spark, [("M1", datetime(2013, 3, 4, 10, 41), "0.1", 1), ("M1", T, "0.2", 2)])
    assert sorted(r.clock_ok for r in rows) == [False, True]


# ---------- quality flags and Parquet in clean (T-30.4) ----------

def cleaned(spark, rows):
    from jobs.clean_readings import clean

    return {r.seq: r for r in clean(frame(spark, rows)).collect()}


def test_each_defect_gets_its_flag_and_no_energy(spark):
    rows = cleaned(spark, [
        ("M1", datetime(2013, 3, 4, 0, 0), "0.25", 1),
        ("M1", datetime(2013, 3, 4, 0, 30), None, 2),
        ("M1", datetime(2013, 3, 4, 1, 0), "Null", 3),
        ("M1", datetime(2013, 3, 4, 1, 30), "-1.5", 4),
        ("M1", datetime(2013, 3, 4, 2, 0), "25.01", 5),
        ("M1", datetime(2013, 3, 4, 2, 30), "abc", 6),
        ("M1", datetime(2013, 3, 4, 3, 11), "0.2", 7),
        ("M1", datetime(2013, 3, 4, 3, 30), "0", 8),
        ("M1", datetime(2013, 3, 4, 4, 0), "25", 9),
    ])
    flags = {seq: (r.quality_flag, r.is_valid, r.energy_kwh) for seq, r in rows.items()}
    assert flags == {
        1: ("ok", True, 0.25),
        2: ("null_value", False, None),
        3: ("null_value", False, None),
        4: ("negative", False, None),
        5: ("out_of_range", False, None),
        6: ("not_a_number", False, None),
        7: ("invalid_clock", False, None),
        8: ("ok", True, 0.0),        # zero is a real reading (RN-07)
        9: ("ok", True, 25.0),       # the limit itself is allowed
    }
    assert rows[1].energy_text == "0.25" and rows[3].energy_text == "Null"  # the original text is kept


def test_clean_partition_date_is_the_reading_date(spark):
    rows = cleaned(spark, [("M1", datetime(2013, 3, 4, 23, 57), "0.2", 1),
                           ("M1", datetime(2013, 3, 4, 23, 41), "0.2", 2)])
    assert str(rows[1].date) == "2013-03-05"  # normalized to 00:00 of the next day
    assert str(rows[2].date) == "2013-03-04"  # invalid clock: date of the original time


def test_job_writes_parquet_partitioned_by_date_with_success_markers(spark, tmp_path):
    from jobs.clean_readings import merge_and_write

    path = str(tmp_path / "clean")
    raw = frame(spark, [("M1", datetime(2013, 3, 4, 10, 0), "0.2", 1), ("M1", datetime(2013, 3, 5, 10, 0), "0.3", 2)])
    _, dates = merge_and_write(spark, raw, path)
    assert [str(d) for d in dates] == ["2013-03-04", "2013-03-05"]
    for d in dates:
        folder = tmp_path / "clean" / f"date={d}"
        assert list(folder.glob("*.snappy.parquet"))
        assert (folder / "_SUCCESS").exists()
    assert spark.read.parquet(path).count() == 2


def test_a_resend_is_merged_with_what_clean_already_had(spark, tmp_path):
    from jobs.clean_readings import merge_and_write

    path = str(tmp_path / "clean")
    t = datetime(2013, 3, 4, 10, 0)
    merge_and_write(spark, frame(spark, [("M1", t, "0.2", 1), ("M2", t, "0.4", 1)]), path)
    # later: a resend brings M1 again with a higher sequence, and M3 for the same date
    merge_and_write(spark, frame(spark, [("M1", t, "0.9", 2), ("M3", t, "0.1", 1)]), path)
    rows = {r.meter_id: r.energy_kwh for r in spark.read.parquet(path).collect()}
    assert rows == {"M1": 0.9, "M2": 0.4, "M3": 0.1}  # M2 was not lost, M1 took the higher sequence


def test_running_the_same_input_twice_gives_the_same_clean_data(spark, tmp_path):
    from jobs.clean_readings import merge_and_write

    path = str(tmp_path / "clean")
    raw = frame(spark, [("M1", datetime(2013, 3, 4, h, 0), "0.2", h) for h in range(24)])
    merge_and_write(spark, raw, path)
    first = sorted(spark.read.parquet(path).collect())
    merge_and_write(spark, raw, path)
    assert sorted(spark.read.parquet(path).collect()) == first
