"""Quarterly report (US-54, T-54.1): figures per transformer and the reading-quality shares."""

import csv
import os
from datetime import date, datetime
from decimal import Decimal

import pytest

from jobs.inventory import write_raw
from jobs.lake import Lake
from jobs.quarterly_report import build_report, quarter_arg, quarter_days, write_report

INGEST = date(2026, 11, 1)
F4_METERS = ["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
             "circuit_capacity_kva"]
FACT_COLUMNS = "date date, transformer_code string, delivered_kwh double, metered_kwh double, loss_kwh double, " \
               "loss_cost decimal(12,2)"
FILLED_COLUMNS = "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean, " \
                 "estimation_method string, quality_flag string, date date"


def build_lake(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    meters = [("M-1", "C-1", "TR-1", "100.00", "CI-1", "300.00"),
              ("M-2", "C-2", "TR-1", "100.00", "CI-1", "300.00"),
              ("M-3", "C-3", "TR-2", "50.00", "CI-1", "300.00")]
    write_raw(lake, {"meters": (F4_METERS, meters)}, INGEST)

    fact = spark.createDataFrame([
        (date(2013, 1, 10), "TR-1", 100.0, 90.0, 10.0, Decimal("1.50")),
        (date(2013, 2, 10), "TR-1", 100.0, 95.0, 5.0, Decimal("0.75")),
        (date(2013, 2, 11), "TR-2", 50.0, 50.0, 0.0, Decimal("0.00")),
        (date(2013, 4, 2), "TR-1", 100.0, 80.0, 20.0, Decimal("3.00")),  # next quarter: must not count in Q1
    ], FACT_COLUMNS)
    fact.write.mode("overwrite").partitionBy("date").parquet(lake.curated("fact_transformer_loss_daily"))

    # Half hours: M-1 has 3 real, 1 gap (missing) and 1 flagged (null_value, estimated); M-2 has 2 real;
    # M-3 has 1 real. Day 2013-04-02 is outside Q1 and must not count.
    rows = [
        ("M-1", datetime(2013, 1, 10, 0, 0), 0.5, False, None, "ok", date(2013, 1, 10)),
        ("M-1", datetime(2013, 1, 10, 0, 30), 0.5, False, None, "ok", date(2013, 1, 10)),
        ("M-1", datetime(2013, 1, 10, 1, 0), 0.5, False, None, "ok", date(2013, 1, 10)),
        ("M-1", datetime(2013, 1, 10, 1, 30), 0.4, True, "linear", "missing", date(2013, 1, 10)),
        ("M-1", datetime(2013, 1, 10, 2, 0), 0.4, True, "linear", "null_value", date(2013, 1, 10)),
        ("M-2", datetime(2013, 1, 10, 0, 0), 0.6, False, None, "ok", date(2013, 1, 10)),
        ("M-2", datetime(2013, 1, 10, 0, 30), 0.6, False, None, "ok", date(2013, 1, 10)),
        ("M-3", datetime(2013, 2, 11, 0, 0), 0.9, False, None, "ok", date(2013, 2, 11)),
        ("M-1", datetime(2013, 4, 2, 0, 0), 0.9, False, None, "ok", date(2013, 4, 2)),
    ]
    spark.createDataFrame(rows, FILLED_COLUMNS).write.mode("overwrite").partitionBy("date") \
        .parquet(lake.clean("readings", "interval_30min_filled"))
    return lake


def q1_fact(spark, lake):
    from pyspark.sql import functions as F
    return spark.read.parquet(lake.curated("fact_transformer_loss_daily")).filter(
        F.col("date").isin(quarter_days("2013-Q1")))


def test_quarter_days_cover_the_whole_quarter():
    assert len(quarter_days("2013-Q1")) == 90  # 2013 is not a leap year
    assert quarter_days("2012-Q1")[-1] == date(2012, 3, 31)  # 2012 is
    assert quarter_days("2013-Q4")[0] == date(2013, 10, 1)
    assert quarter_days("2013-Q4")[-1] == date(2013, 12, 31)


def test_quarter_argument_rejects_anything_but_year_and_quarter():
    assert quarter_arg("2013-Q3") == "2013-Q3"
    for bad in ["2013-Q5", "2013Q1", "2013-1", "Q1-2013", ""]:
        with pytest.raises(Exception):
            quarter_arg(bad)


def test_losses_are_summed_per_transformer_for_the_quarter(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    report = build_report(spark, lake, "2013-Q1", q1_fact(spark, lake)).orderBy("transformer_code").collect()
    tr1, tr2 = report
    assert tr1["transformer_code"] == "TR-1" and tr2["transformer_code"] == "TR-2"
    assert tr1["delivered_kwh"] == 200.0 and tr1["loss_kwh"] == 15.0  # April row not included
    assert round(tr1["loss_pct"], 4) == 0.075
    assert tr1["loss_cost"] == Decimal("2.25")
    assert tr2["loss_kwh"] == 0.0 and tr2["loss_pct"] == 0.0


def test_reading_quality_counts_and_shares(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    tr1 = build_report(spark, lake, "2013-Q1", q1_fact(spark, lake)).filter("transformer_code = 'TR-1'").first()
    assert (tr1["real_readings"], tr1["estimated_readings"], tr1["invalid_readings"]) == (5, 1, 1)
    shares = (round(tr1["share_real"], 4), round(tr1["share_estimated"], 4), round(tr1["share_invalid"], 4))
    assert shares == (0.7143, 0.1429, 0.1429)


def test_circuit_is_carried_from_the_meters(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    tr2 = build_report(spark, lake, "2013-Q1", q1_fact(spark, lake)).filter("transformer_code = 'TR-2'").first()
    assert tr2["circuit_code"] == "CI-1"
    assert tr2["quarter"] == "2013-Q1"


def test_csv_has_a_header_and_one_row_per_transformer(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    report = build_report(spark, lake, "2013-Q1", q1_fact(spark, lake))
    write_report(report, lake, "2013-Q1")
    folder = lake.curated("quarterly_report_csv") + "/quarter=2013-Q1"
    parts = [p for p in os.listdir(folder) if p.endswith(".csv")]
    assert len(parts) == 1
    with open(f"{folder}/{parts[0]}", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0][:3] == ["quarter", "transformer_code", "circuit_code"]
    assert len(rows) == 3  # header + TR-1 + TR-2
