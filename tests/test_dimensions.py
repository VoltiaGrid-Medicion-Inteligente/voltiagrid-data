"""Dimensions of the analytics layer (US-52, T-52.1): keys, grain and rows of each dimension."""

from datetime import date
from decimal import Decimal

from jobs.dimensions import (customer_dim, date_dim, dr_event_dim, meter_dim, month_dim, tariff_band_dim,
                             time_of_day_dim, transformer_dim, write_dim)
from jobs.dr_events import EVENT_SCHEMA, read_events
from jobs.inventory import read_contracts, read_meters, write_raw
from jobs.lake import Lake
from jobs.tariff_bands import read_tariffs

INGEST = date(2026, 11, 1)
F4_METERS = ["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
             "circuit_capacity_kva"]
F4_CONTRACTS = ["customer_code", "contract_code", "tariff_type", "start_date", "end_date"]
F7_CSV = ("valid_from,valid_to,start_time,end_time,band_name,price_per_kwh\n"
          "2012-12-01,2015-12-31,00:00:00,07:59:59,off_peak,0.0500\n"
          "2012-12-01,2015-12-31,08:00:00,23:59:59,peak,0.1500\n")


def build_f4(lake):
    meters = [("M-1", "C-1", "TR-1", "100.00", "CI-1", "300.00"),
              ("M-2", "C-2", "TR-1", "100.00", "CI-1", "300.00"),
              ("M-3", "C-3", "TR-2", "50.00", "CI-1", "300.00")]
    contracts = [("C-1", "K-1", "standard", "2012-01-01", "2013-06-30"),
                 ("C-1", "K-2", "standard", "2013-07-01", ""),  # a change of contract: two versions of C-1
                 ("C-2", "K-3", "tariff_b", "2012-01-01", ""),
                 ("C-3", "K-4", "standard", "2012-01-01", "")]
    write_raw(lake, {"meters": (F4_METERS, meters), "contracts": (F4_CONTRACTS, contracts)}, INGEST)


def build(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    build_f4(lake)
    tariffs_csv = tmp_path / "tariffs.csv"
    tariffs_csv.write_text(F7_CSV)
    meters = read_meters(spark, lake, INGEST.isoformat())
    contracts = read_contracts(spark, lake, INGEST.isoformat())
    tariffs = read_tariffs(spark, str(tariffs_csv))
    return lake, meters, contracts, tariffs


def test_date_dim_has_one_row_per_day_and_unique_keys(spark):
    dates = date_dim(spark, "2013-01-01", "2013-12-31")
    assert dates.count() == 365
    assert dates.select("date_key").distinct().count() == 365
    row = dates.filter("date_key = 20130101").first()
    assert row["day_of_week"] == 2  # 2013-01-01 was a Tuesday (ISO: 1 = Monday)
    assert row["year_month"] == "2013-01"


def test_date_dim_business_days_are_monday_to_friday(spark):
    dates = date_dim(spark, "2013-01-05", "2013-01-13")  # Saturday to Sunday of the following week
    flags = {r["date"]: r["is_business_day"] for r in dates.collect()}
    assert flags[date(2013, 1, 5)] is False  # Saturday
    assert flags[date(2013, 1, 7)] is True  # Monday
    assert flags[date(2013, 1, 11)] is True  # Friday
    assert flags[date(2013, 1, 12)] is False  # Saturday


def test_month_dim_rolls_up_the_days(spark):
    months = month_dim(date_dim(spark, "2013-01-01", "2013-12-31"))
    assert months.count() == 12
    jan = months.filter("year_month = '2013-01'").first()
    assert jan["month_name"] == "January"
    assert jan["first_date_key"] == 20130101


def test_time_of_day_dim_has_the_48_slots_time_key_produces(spark):
    slots = time_of_day_dim(spark)
    assert slots.count() == 48
    assert slots.filter("hh_mm = '00:00'").first()["time_key"] == 0
    assert slots.filter("hh_mm = '23:30'").first()["time_key"] == 47


def test_tariff_band_dim_keeps_every_calendar_row(spark, tmp_path):
    _, _, _, tariffs = build(spark, tmp_path)
    bands = tariff_band_dim(tariffs)
    assert bands.count() == 2
    assert {r["band_name"] for r in bands.collect()} == {"off_peak", "peak"}
    assert bands.filter("band_name = 'peak'").first()["price_per_kwh"] == Decimal("0.1500")


def test_customer_dim_is_type_2_with_one_current_version(spark, tmp_path):
    _, _, contracts, _ = build(spark, tmp_path)
    customers = customer_dim(contracts)
    assert customers.count() == 4
    c1 = customers.filter("customer_code = 'C-1'").orderBy("valid_from").collect()
    assert [r["contract_code"] for r in c1] == ["K-1", "K-2"]
    assert c1[0]["valid_to"] == date(2013, 6, 30) and c1[0]["is_current"] == 0
    assert c1[1]["valid_to"] == date(9999, 12, 31) and c1[1]["is_current"] == 1
    assert customers.filter("is_current = 1").select("customer_code").distinct().count() == 3


def test_meter_and_transformer_dims_have_one_row_per_key(spark, tmp_path):
    _, meters, _, _ = build(spark, tmp_path)
    meter_rows = meter_dim(meters)
    assert meter_rows.count() == 3
    assert meter_rows.select("lclid").distinct().count() == 3
    transformers = transformer_dim(meters)
    assert transformers.count() == 2
    assert transformers.select("transformer_code").distinct().count() == 2


def build_events(spark, lake):
    from datetime import datetime
    events = [
        ("DR-CI-1-20130722", "CI-1", datetime(2013, 7, 22, 18, 0), datetime(2013, 7, 22, 21, 0), 3.0, 31.5, 92.0,
         date(2013, 7, 22)),
        ("DR-CI-2-20130723", "CI-2", datetime(2013, 7, 23, 19, 30), datetime(2013, 7, 23, 21, 0), 1.5, 29.0, 88.5,
         date(2013, 7, 23)),
    ]
    spark.createDataFrame(events, EVENT_SCHEMA).write.mode("overwrite").partitionBy("date") \
        .parquet(lake.curated("dr_events"))


def test_dr_event_dim_has_one_row_per_event_with_its_window_and_values(spark, tmp_path):
    lake, _, _, _ = build(spark, tmp_path)
    build_events(spark, lake)
    events = dr_event_dim(read_events(spark, lake))
    assert events.count() == 2
    assert events.select("event_id").distinct().count() == 2
    assert events.columns == ["event_id", "circuit_code", "start_ts", "end_ts", "duration_h", "forecast_temp_c",
                              "projected_load_pct"]
    row = events.filter("event_id = 'DR-CI-1-20130722'").first()
    assert row["circuit_code"] == "CI-1"
    assert row["duration_h"] == 3.0
    assert row["forecast_temp_c"] == 31.5 and row["projected_load_pct"] == 92.0


def test_write_dim_is_a_single_file_and_a_rerun_gives_the_same_rows(spark, tmp_path):
    lake, meters, _, _ = build(spark, tmp_path)
    first = write_dim(meter_dim(meters), lake, "meter")
    again = write_dim(meter_dim(meters), lake, "meter")
    assert first == again == 3
    back = spark.read.parquet(lake.curated("dim_meter"))
    assert back.count() == 3
