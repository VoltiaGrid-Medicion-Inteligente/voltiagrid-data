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
