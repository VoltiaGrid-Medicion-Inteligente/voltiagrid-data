"""Tariff band by effective date (US-35): calendar join (T-35.1)."""

from datetime import datetime
from decimal import Decimal

import pytest

from jobs.tariff_bands import assign_bands, missing_bands, read_tariffs


def readings(spark, times):
    return spark.createDataFrame([("M1", t, 1.0) for t in times],
                                 "meter_id string, interval_start timestamp_ntz, energy_kwh double")


@pytest.fixture
def tariffs(spark):
    return read_tariffs(spark)


def test_each_interval_gets_the_band_and_price_valid_on_its_date_and_time(spark, tariffs):
    times = [datetime(2013, 1, 1, 7, 30), datetime(2013, 1, 1, 8, 0), datetime(2013, 1, 1, 17, 30),
             datetime(2013, 1, 1, 18, 0), datetime(2013, 1, 1, 21, 30), datetime(2013, 1, 1, 22, 0)]
    rows = {r.interval_start: (r.band_name, r.price_per_kwh) for r in assign_bands(readings(spark, times), tariffs).collect()}
    assert [rows[t] for t in times] == [
        ("off-peak", Decimal("0.1200")), ("intermediate", Decimal("0.1800")), ("intermediate", Decimal("0.1800")),
        ("peak", Decimal("0.3000")), ("peak", Decimal("0.3000")), ("off-peak", Decimal("0.1200")),
    ]


def test_same_hour_in_two_years_gets_the_price_of_its_own_period(spark, tariffs):
    times = [datetime(2011, 12, 31, 19, 0), datetime(2012, 1, 1, 19, 0)]
    rows = {r.interval_start: r for r in assign_bands(readings(spark, times), tariffs).collect()}
    assert rows[times[0]].price_per_kwh == Decimal("0.2500") and str(rows[times[0]].tariff_valid_from) == "2011-01-01"
    assert rows[times[1]].price_per_kwh == Decimal("0.3000") and str(rows[times[1]].tariff_valid_from) == "2012-01-01"


def test_every_row_keeps_its_columns_and_gets_exactly_one_band(spark, tariffs):
    times = [datetime(2013, 1, 1) + i * (datetime(2013, 1, 1, 0, 30) - datetime(2013, 1, 1)) for i in range(48)]
    banded = assign_bands(readings(spark, times), tariffs)
    assert banded.count() == 48
    assert missing_bands(banded) == 0
    assert {"meter_id", "energy_kwh", "band_name", "price_per_kwh", "tariff_valid_from"} <= set(banded.columns)


def test_a_date_outside_the_calendar_is_counted_as_missing(spark, tariffs):
    banded = assign_bands(readings(spark, [datetime(2010, 6, 1, 12, 0)]), tariffs)
    assert missing_bands(banded) == 1


# ---------- T-35.2: monthly consumption per customer and band ----------

def filled_frame(spark, rows):
    """rows: (meter_id, interval_start, kwh, is_estimated)"""
    return spark.createDataFrame(rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, "
                                       "is_estimated boolean")


def meters_frame(spark, pairs):
    return spark.createDataFrame(
        [(m, c, "TR-1", Decimal("100"), "CT-1", Decimal("1000")) for m, c in pairs],
        "meter_id string, customer_code string, transformer_code string, transformer_capacity_kva decimal(8,2), "
        "circuit_code string, circuit_capacity_kva decimal(8,2)")


def test_monthly_consumption_sums_kwh_and_amount_per_customer_and_band(spark, tariffs):
    from jobs.fact_consumption import monthly_consumption

    filled = filled_frame(spark, [
        ("M1", datetime(2013, 1, 5, 19, 0), 1.0, False),   # peak 0.30
        ("M1", datetime(2013, 1, 6, 19, 30), 2.0, True),   # peak, estimated
        ("M1", datetime(2013, 1, 5, 3, 0), 0.5, False),    # off-peak 0.12
        ("M2", datetime(2013, 1, 5, 10, 0), 4.0, False),   # intermediate 0.18
    ])
    rows = {(r.customer_code, r.band_name): r for r in
            monthly_consumption(filled, meters_frame(spark, [("M1", "CU-1"), ("M2", "CU-2")]), tariffs).collect()}
    peak = rows[("CU-1", "peak")]
    assert (peak.energy_kwh, peak.estimated_kwh, peak.intervals, peak.estimated_intervals) == (3.0, 2.0, 2, 1)
    assert peak.amount == Decimal("0.90") and peak.month == "2013-01"
    assert rows[("CU-1", "off-peak")].amount == Decimal("0.06")
    assert rows[("CU-2", "intermediate")].amount == Decimal("0.72")
    assert len(rows) == 3


# ---------- T-35.3: a tariff change in the middle of the month ----------

def mid_month_tariffs(spark):
    """Prices go up on 2013-03-15: two periods inside March."""
    from datetime import date as d

    from schemas.f7 import F7_SCHEMA

    rows = []
    for start, end, prices in (("2013-01-01", "2013-03-14", ("0.12", "0.18", "0.30")),
                               ("2013-03-15", "2099-12-31", ("0.14", "0.20", "0.35"))):
        off, mid, peak = prices
        rows += [(start, end, "00:00:00", "07:59:59", "off-peak", off), (start, end, "08:00:00", "17:59:59", "intermediate", mid),
                 (start, end, "18:00:00", "21:59:59", "peak", peak), (start, end, "22:00:00", "23:59:59", "off-peak", off)]
    typed = [(d.fromisoformat(a), d.fromisoformat(b), s, e, n, Decimal(p)) for a, b, s, e, n, p in rows]
    return spark.createDataFrame(typed, F7_SCHEMA)


def test_tariff_change_in_the_middle_of_the_month_splits_it(spark):
    from jobs.fact_consumption import monthly_consumption

    filled = filled_frame(spark, [
        ("M1", datetime(2013, 3, 14, 19, 0), 1.0, False),   # last day of the old price: peak 0.30
        ("M1", datetime(2013, 3, 14, 19, 30), 1.0, False),
        ("M1", datetime(2013, 3, 15, 19, 0), 2.0, False),   # first day of the new price: peak 0.35
        ("M1", datetime(2013, 3, 20, 10, 0), 1.0, False),   # intermediate, new price only
    ])
    rows = monthly_consumption(filled, meters_frame(spark, [("M1", "CU-1")]), mid_month_tariffs(spark)).collect()
    peak = sorted((r for r in rows if r.band_name == "peak"), key=lambda r: r.tariff_valid_from)
    assert [(str(r.tariff_valid_from), r.price_per_kwh, r.energy_kwh, r.amount) for r in peak] == [
        ("2013-01-01", Decimal("0.3000"), 2.0, Decimal("0.60")),
        ("2013-03-15", Decimal("0.3500"), 2.0, Decimal("0.70")),
    ]
    assert {r.month for r in rows} == {"2013-03"}
    assert sum(r.amount for r in rows) == Decimal("1.50")  # 0.60 + 0.70 + 0.20


def test_without_a_change_each_band_is_one_row_per_month(spark, tariffs):
    from jobs.fact_consumption import monthly_consumption

    filled = filled_frame(spark, [("M1", datetime(2013, 3, d, 19, 0), 1.0, False) for d in range(1, 32)])
    rows = monthly_consumption(filled, meters_frame(spark, [("M1", "CU-1")]), tariffs).collect()
    assert len(rows) == 1 and rows[0].energy_kwh == 31.0


def test_the_job_stops_when_a_date_has_no_band(spark, tmp_path):
    from datetime import date

    from pyspark.sql import functions as F

    from jobs.estimate_gaps import filled_path
    from jobs.fact_consumption import run
    from jobs.inventory import write_raw
    from jobs.lake import Lake

    lake = Lake.local(str(tmp_path / "lake"))
    filled = filled_frame(spark, [("M1", datetime(2010, 6, 1, 12, 0), 1.0, False)]).withColumn(
        "date", F.to_date("interval_start"))
    filled.write.partitionBy("date").parquet(filled_path(lake))
    write_raw(lake, {"meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva",
                                 "circuit_code", "circuit_capacity_kva"],
                                [("M1", "CU-1", "TR-1", Decimal("1"), "CT-1", Decimal("1"))])}, date(2026, 11, 1))
    with pytest.raises(SystemExit, match="no tariff band"):
        run(spark, lake, ["2010-06"])
