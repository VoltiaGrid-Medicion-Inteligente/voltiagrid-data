"""Consumption fact per meter and 30-minute interval (US-48, T-48.1)."""

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from jobs.lake import Lake

DAY = date(2013, 1, 15)


def build_lake(spark, tmp_path, meters=("M1", "M2"), f4_meters=("M1", "M2")):
    from jobs.estimate_gaps import filled_path
    from jobs.inventory import write_raw

    lake = Lake.local(str(tmp_path / "lake"))
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"],
                   [(m, f"CU-{m}", "TR-1", Decimal("100"), "CT-1", Decimal("1000")) for m in f4_meters]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"],
                      [(f"CU-{m}", f"CT-{m}", "standard", date(2011, 11, 1), None) for m in f4_meters]),
    }, date(2026, 11, 1))
    rows = []
    for m in meters:
        ts = datetime.combine(DAY, datetime.min.time())
        for i in range(48):
            estimated = m == "M2" and i in (37, 38)
            rows.append((m, ts, 0.5, estimated, "interpolation" if estimated else None, "ok", DAY))
            ts += timedelta(minutes=30)
    spark.createDataFrame(rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean, "
                                "estimation_method string, quality_flag string, date date") \
        .write.partitionBy("date").parquet(filled_path(lake))
    return lake


def test_date_key_and_time_key(spark):
    from jobs.analytics import date_key, time_key

    df = spark.createDataFrame([(datetime(2013, 1, 15, 18, 30), DAY)], "ts timestamp_ntz, d date")
    row = df.select(date_key("d").alias("dk"), time_key("ts").alias("tk")).first()
    assert (row.dk, row.tk) == (20130115, 37)


def test_one_row_per_meter_and_interval_with_band_cost_and_estimation(spark, tmp_path):
    from jobs.fact_consumption_interval import run

    rows = run(spark, build_lake(spark, tmp_path), [str(DAY)]).collect()
    assert len(rows) == 2 * 48
    peak = next(r for r in rows if r.meter_id == "M2" and r.time_key == 37)       # 18:30, 2012+ peak price 0.30
    assert (peak.date_key, peak.band_name, peak.customer_code, peak.transformer_code) == (20130115, "peak", "CU-M2", "TR-1")
    assert (peak.energy_cost, peak.is_estimated, peak.estimation_method) == (Decimal("0.1500"), 1, "interpolation")
    night = next(r for r in rows if r.meter_id == "M1" and r.time_key == 0)
    assert (night.band_name, night.energy_cost, night.is_estimated, night.estimation_method) == \
        ("off-peak", Decimal("0.0600"), 0, "none")


def test_kwh_of_the_fact_equals_the_filled_readings(spark, tmp_path):
    from jobs.fact_consumption_interval import run

    total = run(spark, build_lake(spark, tmp_path), [str(DAY)]).agg({"energy_kwh": "sum"}).first()[0]
    assert total == 2 * 48 * 0.5


def test_a_meter_missing_from_f4_stops_the_job(spark, tmp_path):
    from jobs.fact_consumption_interval import run

    lake = build_lake(spark, tmp_path, meters=("M1", "M2"), f4_meters=("M1",))
    with pytest.raises(SystemExit, match="orphan keys.*M2"):
        run(spark, lake, [str(DAY)])
