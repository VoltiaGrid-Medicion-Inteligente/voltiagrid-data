"""Circuit load per interval (US-33, T-33.1)."""

from datetime import datetime
from decimal import Decimal

from jobs.circuit_load import circuit_load

T = datetime(2013, 1, 1, 18, 0)


def meters_frame(spark, rows):
    return spark.createDataFrame(
        [(m, f"CU-{m}", "TR-1", Decimal("100"), c, Decimal(str(cap))) for m, c, cap in rows],
        "meter_id string, customer_code string, transformer_code string, transformer_capacity_kva decimal(8,2), "
        "circuit_code string, circuit_capacity_kva decimal(8,2)")


def readings_frame(spark, rows):
    return spark.createDataFrame(
        [(m, ts, kwh, ok) for m, ts, kwh, ok in rows],
        "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_valid boolean")


def test_load_is_twice_the_half_hour_energy_over_the_capacity(spark):
    meters = meters_frame(spark, [("M1", "CT-1", 10), ("M2", "CT-1", 10)])
    readings = readings_frame(spark, [("M1", T, 2.0, True), ("M2", T, 2.5, True)])
    row = circuit_load(readings, meters).first()
    assert row.energy_kwh == 4.5
    assert row.load_kw == 9.0          # 4.5 kWh in 30 minutes = 9 kW on average
    assert row.load_pct == 0.9
    assert row.is_overloaded is True
    assert (row.meters_reporting, row.meters_total) == (2, 2)


# ---------- T-33.3: edges and the whole job ----------

def test_exactly_85_percent_is_not_overloaded_and_just_above_is(spark):
    meters = meters_frame(spark, [("M1", "CT-1", 100), ("M2", "CT-2", 100)])
    readings = readings_frame(spark, [("M1", T, 42.5, True), ("M2", T, 42.51, True)])  # 85.0% and 85.02%
    rows = {r.circuit_code: r for r in circuit_load(readings, meters).collect()}
    assert (rows["CT-1"].load_pct, rows["CT-1"].is_overloaded) == (0.85, False)
    assert (rows["CT-2"].load_pct, rows["CT-2"].is_overloaded) == (0.8502, True)


def test_invalid_readings_do_not_add_load(spark):
    meters = meters_frame(spark, [("M1", "CT-1", 10), ("M2", "CT-1", 10)])
    readings = readings_frame(spark, [("M1", T, 1.0, True), ("M2", T, None, False)])
    row = circuit_load(readings, meters).first()
    assert row.energy_kwh == 1.0
    assert (row.meters_reporting, row.meters_total) == (1, 2)


def test_meters_not_in_the_inventory_are_left_out(spark):
    meters = meters_frame(spark, [("M1", "CT-1", 10)])
    readings = readings_frame(spark, [("M1", T, 1.0, True), ("UNKNOWN", T, 9.0, True)])
    assert circuit_load(readings, meters).first().energy_kwh == 1.0


def test_each_circuit_and_interval_is_one_row(spark):
    meters = meters_frame(spark, [("M1", "CT-1", 10), ("M2", "CT-2", 20)])
    t2 = datetime(2013, 1, 1, 18, 30)
    readings = readings_frame(spark, [("M1", T, 1.0, True), ("M1", t2, 2.0, True), ("M2", T, 3.0, True)])
    keys = sorted((r.circuit_code, r.interval_start) for r in circuit_load(readings, meters).collect())
    assert keys == [("CT-1", T), ("CT-1", t2), ("CT-2", T)]


def test_job_reads_the_dates_of_a_cleaning_run_and_writes_curated(spark, tmp_path):
    import argparse
    import json
    from datetime import date

    from jobs.circuit_load import run
    from jobs.clean_readings import clean_path, merge_and_write
    from jobs.inventory import write_raw
    from jobs.lake import Lake, dates_from_args, write_text
    from jobs.readings import READING_COLUMNS

    lake = Lake.local(str(tmp_path / "lake"))
    write_raw(lake, {"meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva",
                                 "circuit_code", "circuit_capacity_kva"],
                                [("M1", "CU-M1", "TR-1", Decimal("50"), "CT-1", Decimal("4"))])}, date(2026, 11, 1))
    raw = spark.createDataFrame([("M1", T, "1.8", 1, "f3"), ("M1", datetime(2013, 1, 2, 9, 0), "0.5", 2, "f3")],
                                "meter_id string, original_ts timestamp_ntz, energy_text string, seq long, "
                                "source string").select(READING_COLUMNS)
    merge_and_write(spark, raw, clean_path(lake))
    write_text(spark, f"{clean_path(lake)}/_runs/f3-test.json", json.dumps({"dates": ["2013-01-01", "2013-01-02"]}))

    dates = dates_from_args(spark, argparse.Namespace(date=None, run="f3-test"), lake)
    rows = {r.interval_start: r for r in run(spark, lake, dates).collect()}
    assert rows[T].load_pct == 0.9 and rows[T].is_overloaded is True      # 1.8 kWh -> 3.6 kW of 4 kVA
    assert rows[datetime(2013, 1, 2, 9, 0)].is_overloaded is False         # 1.0 kW of 4 kVA
    assert (tmp_path / "lake" / "curated" / "dataset=circuit_load" / "date=2013-01-01" / "_SUCCESS").exists()
