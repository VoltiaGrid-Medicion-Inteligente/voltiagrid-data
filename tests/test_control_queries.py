"""Control queries (CT-10): the answers of questions 1 to 6 on a small lake whose answers are known."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from jobs.control_queries import (month_bounds, q1_consumption_by_band, q2_transformer_losses,
                                  q3_estimated_readings, q4_dr_savings, q5_temperature_and_peak_load,
                                  q6_customers_not_billed)
from jobs.inventory import write_raw
from jobs.lake import Lake

INGEST = date(2026, 11, 1)
F4_METERS = ["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
             "circuit_capacity_kva"]
F4_CONTRACTS = ["customer_code", "contract_code", "tariff_type", "start_date", "end_date"]
CONSUMPTION_COLUMNS = ("customer_code string, band_name string, tariff_valid_from date, price_per_kwh decimal(8,4), "
                       "energy_kwh double, estimated_kwh double, intervals int, estimated_intervals int, "
                       "amount decimal(12,2), month string")
LOSS_COLUMNS = ("date_key int, transformer_code string, circuit_code string, delivered_kwh double, metered_kwh double, "
                "estimated_kwh double, loss_kwh double, loss_pct double, loss_cost decimal(12,2), "
                "consecutive_days_over_12 int, is_suspicious int, suspicious_rank int, date date")
DR_COLUMNS = ("date_key int, event_id string, customer_code string, circuit_code string, baseline_kwh double, "
              "actual_kwh double, savings_kwh double, savings_pct double, incentive_paid int, "
              "incentive_amount decimal(12,2), date date")
FILLED_COLUMNS = "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean, " \
                 "estimation_method string, quality_flag string, date date"
ORDER_COLUMNS = ("order_id string, customer_code string, month string, reason string, percentage double, "
                 "created_at timestamp_ntz")
STATUS_COLUMNS = "customer_code string, month string, estimated_pct double, is_billable boolean, inspection_order string"
LOAD_WEATHER_COLUMNS = ("date_key int, hour int, transformer_code string, circuit_code string, delivered_kwh double, "
                        "peak_kw double, load_pct_capacity double, temperature_c double, date date")


def build_lake(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    meters = [("M-1", "C-1", "TR-1", "100.00", "CI-1", "300.00"),
              ("M-2", "C-1", "TR-1", "100.00", "CI-1", "300.00"),
              ("M-3", "C-2", "TR-2", "50.00", "CI-1", "300.00")]
    contracts = [("C-1", "K-1", "standard", "2012-01-01", ""),
                 ("C-2", "K-2", "tariff_b", "2012-01-01", "")]
    write_raw(lake, {"meters": (F4_METERS, meters), "contracts": (F4_CONTRACTS, contracts)}, INGEST)

    consumption = spark.createDataFrame([
        ("C-1", "peak", date(2013, 1, 1), Decimal("0.1500"), 10.0, 0.0, 4, 0, Decimal("1.50"), "2013-01"),
        ("C-1", "off_peak", date(2013, 1, 1), Decimal("0.0500"), 5.0, 0.0, 2, 0, Decimal("0.25"), "2013-01"),
        ("C-2", "peak", date(2013, 1, 1), Decimal("0.1500"), 7.0, 0.0, 2, 0, Decimal("1.05"), "2013-01"),
        ("C-1", "peak", date(2013, 2, 1), Decimal("0.1500"), 99.0, 0.0, 2, 0, Decimal("14.85"), "2013-02"),
    ], CONSUMPTION_COLUMNS)
    consumption.write.mode("overwrite").partitionBy("month").parquet(lake.curated("fact_consumption"))

    loss = spark.createDataFrame([
        (20130101, "TR-1", "CI-1", 100.0, 90.0, 0.0, 10.0, 0.10, Decimal("1.50"), 1, 1, 1, date(2013, 1, 1)),
        (20130102, "TR-1", "CI-1", 100.0, 95.0, 0.0, 5.0, 0.05, Decimal("0.75"), 0, 0, None, date(2013, 1, 2)),
        (20130101, "TR-2", "CI-1", 100.0, 99.0, 0.0, 1.0, 0.01, Decimal("0.10"), 0, 0, None, date(2013, 1, 1)),
    ], LOSS_COLUMNS)
    loss.write.mode("overwrite").partitionBy("date").parquet(lake.curated("fact_transformer_loss_daily"))

    dr = spark.createDataFrame([
        (20130722, "DR-1", "C-1", "CI-1", 10.0, 8.0, 2.0, 0.2, 1, Decimal("1.50"), date(2013, 7, 22)),
        (20130722, "DR-1", "C-2", "CI-1", 10.0, 7.0, 3.0, 0.3, 0, Decimal("0.00"), date(2013, 7, 22)),
    ], DR_COLUMNS)
    dr.write.mode("overwrite").partitionBy("date").parquet(lake.curated("fact_dr_participation"))

    readings = [
        ("M-1", datetime(2013, 1, 1, 0, 0), 0.5, False, None, "ok", date(2013, 1, 1)),
        ("M-1", datetime(2013, 1, 1, 0, 30), 0.5, True, "linear", "missing", date(2013, 1, 1)),
        ("M-1", datetime(2013, 1, 1, 1, 0), 0.5, True, "linear", "null_value", date(2013, 1, 1)),
        ("M-1", datetime(2013, 1, 1, 1, 30), 0.5, False, None, "ok", date(2013, 1, 1)),
        ("M-1", datetime(2013, 1, 1, 2, 0), 0.5, False, None, "ok", date(2013, 1, 1)),
        ("M-1", datetime(2013, 1, 1, 2, 30), 0.5, False, None, "ok", date(2013, 1, 1)),
        ("M-2", datetime(2013, 1, 1, 0, 0), 0.6, False, None, "ok", date(2013, 1, 1)),
        ("M-2", datetime(2013, 1, 1, 0, 30), 0.6, False, None, "ok", date(2013, 1, 1)),
        ("M-3", datetime(2013, 1, 1, 0, 0), 0.9, False, None, "ok", date(2013, 1, 1)),
        ("M-1", datetime(2013, 2, 1, 0, 0), 0.9, True, "linear", "missing", date(2013, 2, 1)),  # February: not in Jan
    ]
    spark.createDataFrame(readings, FILLED_COLUMNS).write.mode("overwrite").partitionBy("date") \
        .parquet(lake.clean("readings", "interval_30min_filled"))

    orders = spark.createDataFrame([
        ("IO-2013-01-C-2", "C-2", "2013-01", "estimated share above 20%", 0.25, datetime(2013, 2, 1, 6, 0)),
    ], ORDER_COLUMNS)
    orders.write.mode("overwrite").partitionBy("month").parquet(lake.curated("inspection_orders"))
    status = spark.createDataFrame([
        ("C-1", "2013-01", 0.0, True, None),
        ("C-2", "2013-01", 0.25, False, "IO-2013-01-C-2"),
    ], STATUS_COLUMNS)
    status.write.mode("overwrite").partitionBy("month").parquet(lake.curated("billing_status"))

    load_weather = spark.createDataFrame([
        (20130101, 0, "TR-1", "CI-1", 2.5, 10.0, 0.1, 0.0, date(2013, 1, 1)),
        (20130101, 1, "TR-1", "CI-1", 5.0, 20.0, 0.2, 5.0, date(2013, 1, 1)),
        (20130102, 0, "TR-1", "CI-1", 7.5, 30.0, 0.3, 10.0, date(2013, 1, 2)),
        (20130102, 1, "TR-2", "CI-1", 11.0, 45.0, 0.9, None, date(2013, 1, 2)),     # F2 has no reading for this hour
        (20130201, 0, "TR-1", "CI-1", 25.0, 99.0, 0.99, 20.0, date(2013, 2, 1)),   # February: not in January
    ], LOAD_WEATHER_COLUMNS)
    load_weather.write.mode("overwrite").partitionBy("date").parquet(lake.curated("fact_load_weather_hourly"))
    return lake


def test_month_bounds_cover_the_whole_month():
    assert month_bounds("2013-02") == (date(2013, 2, 1), date(2013, 2, 28))
    assert month_bounds("2012-02")[1] == date(2012, 2, 29)
    with pytest.raises(ValueError):
        month_bounds("2013-13")


def test_q1_sums_band_and_tariff_type_of_the_month(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    rows = {(r["band_name"], r["tariff_type"]): (r["kwh"], r["amount"])
            for r in q1_consumption_by_band(spark, lake, "2013-01").collect()}
    assert rows == {("off_peak", "standard"): (5.0, Decimal("0.25")),
                    ("peak", "standard"): (10.0, Decimal("1.50")),
                    ("peak", "tariff_b"): (7.0, Decimal("1.05"))}  # February is not in January


def test_q2_top_transformers_by_loss_cost(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    top = q2_transformer_losses(spark, lake, top=1).collect()
    assert [r["transformer_code"] for r in top] == ["TR-1"]
    assert top[0]["loss_kwh"] == 15.0 and top[0]["loss_cost"] == Decimal("2.25")
    assert top[0]["suspicious_days"] == 1
    assert top[0]["avg_loss_pct"] == 0.075


def test_q3_estimated_share_per_transformer_for_the_month(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    rows = {r["transformer_code"]: r for r in q3_estimated_readings(spark, lake, "2013-01").collect()}
    assert rows["TR-1"]["intervals"] == 8                 # M-1 has 6 half hours in January, M-2 has 2
    assert rows["TR-1"]["estimated_intervals"] == 2
    assert rows["TR-1"]["estimated_pct"] == 25.0          # 2 of 8
    assert rows["TR-2"]["estimated_pct"] == 0.0
    assert rows["TR-1"]["month"] == "2013-01"


def test_q4_savings_and_incentive_cost_per_event(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    event = q4_dr_savings(spark, lake).collect()[0]
    assert event["event_id"] == "DR-1"
    assert event["savings_kwh"] == 5.0
    assert event["customers_paid"] == 1
    assert event["incentive_cost"] == Decimal("1.50")


def test_q5_peak_load_and_correlation_of_the_month(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    by_day, totals = q5_temperature_and_peak_load(spark, lake, "2013-01")
    assert totals["rows"] == 4                    # February is not in January
    assert totals["peak_load_pct"] == 0.9         # the hour without temperature still counts for the peak
    assert totals["correlation"] == 1.0           # but not for the correlation: load grows with temperature
    days = {r["date"]: (r["peak_load_pct"], r["max_temp_c"], r["avg_temp_c"]) for r in by_day.collect()}
    assert days == {date(2013, 1, 1): (0.2, 5.0, 2.5), date(2013, 1, 2): (0.9, 10.0, 10.0)}


def test_q5_month_without_rows_has_no_peak(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    by_day, totals = q5_temperature_and_peak_load(spark, lake, "2013-03")
    assert totals == {"rows": 0, "peak_load_pct": None, "correlation": None}
    assert by_day.count() == 0


def test_q6_inspection_orders_agree_with_the_billing_status(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    by_reason, orders, not_billable = q6_customers_not_billed(spark, lake, "2013-01")
    assert orders == not_billable == 1
    reasons = {r["reason"]: r["customers"] for r in by_reason.collect()}
    assert reasons == {"estimated share above 20%": 1}
