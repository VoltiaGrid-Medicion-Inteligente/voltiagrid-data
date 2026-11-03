"""Gap estimation (US-34): full interval grid (T-34.1)."""

from datetime import date, datetime, timedelta

import pytest

from jobs.clean_readings import clean
from jobs.estimate_gaps import complete_grid, fill_gaps
from jobs.readings import READING_COLUMNS

DAY = date(2013, 4, 1)  # a Monday
HALF_HOUR = timedelta(minutes=30)


def clean_rows(spark, readings):
    """readings: (meter_id, timestamp, kwh text) -> clean rows, as the cleaning job writes them."""
    rows = [(m, ts, kwh, i, "f3") for i, (m, ts, kwh) in enumerate(readings, 1)]
    raw = spark.createDataFrame(rows, "meter_id string, original_ts timestamp_ntz, energy_text string, seq long, "
                                "source string").select(READING_COLUMNS)
    return clean(raw)


def day_of(meter, day, kwh="0.3", skip=()):
    start = datetime.combine(day, datetime.min.time())
    return [(meter, start + i * HALF_HOUR, kwh) for i in range(48) if i not in skip]


@pytest.fixture
def one_day_with_holes(spark):
    return clean_rows(spark, day_of("M1", DAY, skip={10, 11, 12}) + [("M1", datetime(2013, 4, 1, 15, 0), "Null")])


def test_grid_has_48_intervals_per_meter_and_day(spark, one_day_with_holes):
    grid = complete_grid(one_day_with_holes, DAY, DAY + timedelta(days=1))
    assert grid.count() == 96
    assert grid.filter("meter_id = 'M1'").select("interval_start").distinct().count() == 96


def test_missing_and_invalid_intervals_are_gaps_in_the_grid(spark, one_day_with_holes):
    rows = {r.interval_start: r for r in complete_grid(one_day_with_holes, DAY, DAY).collect()}
    assert rows[datetime(2013, 4, 1, 5, 0)].actual_kwh is None
    assert rows[datetime(2013, 4, 1, 5, 0)].quality_flag == "missing"
    assert rows[datetime(2013, 4, 1, 15, 0)].actual_kwh is None
    assert rows[datetime(2013, 4, 1, 15, 0)].quality_flag == "null_value"
    assert rows[datetime(2013, 4, 1, 9, 0)].actual_kwh == 0.3


def test_a_meter_that_appears_later_has_no_grid_before_it_existed(spark):
    rows = clean_rows(spark, day_of("OLD", DAY - timedelta(days=1)) + day_of("NEW", DAY))
    grid = complete_grid(rows, DAY - timedelta(days=1), DAY)
    counts = {r.meter_id: r["count"] for r in grid.groupBy("meter_id").count().collect()}
    assert counts == {"OLD": 96, "NEW": 48}


def test_a_meter_with_no_reading_on_the_day_still_gets_its_48_intervals(spark):
    rows = clean_rows(spark, day_of("M1", DAY - timedelta(days=7)))
    filled = fill_gaps(rows, [DAY]).collect()
    assert len(filled) == 48
    assert all(r.is_estimated for r in filled)


def test_filled_output_covers_only_the_requested_days(spark, one_day_with_holes):
    filled = fill_gaps(one_day_with_holes, [DAY])
    assert filled.count() == 48
    assert {r.date for r in filled.select("date").distinct().collect()} == {DAY}
