"""No fact may point to a key that no dimension has (US-52, T-52.2): checked with the real dimensions built by jobs/dimensions.py."""

from datetime import datetime

import pytest

from jobs.analytics import check_no_orphans, date_key, orphan_keys, time_key
from jobs.dimensions import date_dim, meter_dim, time_of_day_dim
from jobs.inventory import read_meters
from jobs.lake import Lake
from tests.test_dimensions import INGEST, build_f4


def fact_rows(spark, rows):
    """Consumption-like fact at half-hour grain: meter, the instant, and the keys derived from it."""
    df = spark.createDataFrame(rows, "lclid string, interval_start timestamp_ntz, energy_kwh double")
    return df.select("lclid", "energy_kwh", date_key("interval_start").alias("date_key"),
                     time_key("interval_start").alias("time_key"))


def test_facts_on_known_dates_meters_and_slots_have_no_orphans(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    build_f4(lake)
    meters = meter_dim(read_meters(spark, lake, INGEST.isoformat()))
    dates = date_dim(spark, "2013-01-01", "2013-12-31")
    slots = time_of_day_dim(spark)
    fact = fact_rows(spark, [("M-1", datetime(2013, 1, 20, 0, 0), 0.5), ("M-2", datetime(2013, 1, 20, 23, 30), 0.7),
                             ("M-3", datetime(2013, 12, 31, 12, 30), 0.1)])
    check_no_orphans(fact, {"lclid": meters, "date_key": dates, "time_key": slots}, "fact_consumption_interval")


def test_orphan_meter_is_reported_by_key(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    build_f4(lake)
    meters = meter_dim(read_meters(spark, lake, INGEST.isoformat()))
    fact = fact_rows(spark, [("M-1", datetime(2013, 1, 20, 0, 0), 0.5), ("M-404", datetime(2013, 1, 20, 0, 0), 0.5)])
    assert orphan_keys(fact, meters, "lclid") == ["M-404"]


def test_date_outside_the_dim_date_range_is_an_orphan(spark):
    dates = date_dim(spark, "2013-01-01", "2013-12-31")
    fact = fact_rows(spark, [("M-1", datetime(2014, 1, 1, 0, 0), 0.5)])
    assert orphan_keys(fact, dates, "date_key") == [20140101]


def test_check_no_orphans_stops_the_job(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    build_f4(lake)
    meters = meter_dim(read_meters(spark, lake, INGEST.isoformat()))
    fact = fact_rows(spark, [("M-999", datetime(2013, 1, 20, 0, 0), 0.5)])
    with pytest.raises(SystemExit):
        check_no_orphans(fact, {"lclid": meters}, "fact_consumption_interval")
