"""Gap estimation (US-34): full interval grid (T-34.1), linear interpolation (T-34.2), 4-week profile (T-34.3)."""

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
    assert all(r.quality_flag == "missing" for r in filled)


def test_filled_output_covers_only_the_requested_days(spark, one_day_with_holes):
    filled = fill_gaps(one_day_with_holes, [DAY])
    assert filled.count() == 48
    assert {r.date for r in filled.select("date").distinct().collect()} == {DAY}


# ---------- T-34.2: linear interpolation ----------

def filled_by_time(spark, readings, day=DAY):
    return {r.interval_start: r for r in fill_gaps(clean_rows(spark, readings), [day]).collect()}


def at(hour, minute=0, day=DAY):
    return datetime.combine(day, datetime.min.time()) + timedelta(hours=hour, minutes=minute)


def test_90_minute_gap_is_interpolated_on_the_line_between_its_neighbours(spark):
    readings = [r for r in day_of("M1", DAY, kwh="0.2") if r[1] not in (at(5), at(5, 30), at(6))]
    readings = [(m, ts, "0.6" if ts == at(6, 30) else kwh) for m, ts, kwh in readings]  # 0.2 at 4:30, 0.6 at 6:30
    rows = filled_by_time(spark, readings)
    assert [rows[at(5)].energy_kwh, rows[at(5, 30)].energy_kwh, rows[at(6)].energy_kwh] == [0.3, 0.4, 0.5]
    assert {rows[t].estimation_method for t in (at(5), at(5, 30), at(6))} == {"interpolation"}
    assert all(rows[t].is_estimated for t in (at(5), at(5, 30), at(6)))
    assert rows[at(4, 30)].is_estimated is False and rows[at(4, 30)].estimation_method is None


def test_2_hour_gap_is_still_interpolated_but_2_and_a_half_is_not(spark):
    four = {at(10), at(10, 30), at(11), at(11, 30)}
    five = {at(15), at(15, 30), at(16), at(16, 30), at(17)}
    rows = filled_by_time(spark, [r for r in day_of("M1", DAY) if r[1] not in four | five])
    assert all(rows[t].estimation_method == "interpolation" for t in four)
    assert all(rows[t].estimation_method != "interpolation" for t in five)


def test_flagged_readings_are_gaps_and_get_interpolated(spark):
    readings = [(m, ts, "-3" if ts == at(8) else kwh) for m, ts, kwh in day_of("M1", DAY, kwh="0.25")]
    row = filled_by_time(spark, readings)[at(8)]
    assert (row.energy_kwh, row.estimation_method, row.quality_flag) == (0.25, "interpolation", "negative")


def test_gap_across_midnight_uses_the_reading_of_the_day_before(spark):
    yesterday = day_of("M1", DAY - timedelta(days=1), kwh="0.4")
    today = day_of("M1", DAY, kwh="0.4", skip={0, 1})
    rows = filled_by_time(spark, yesterday + today)
    assert rows[at(0)].estimation_method == "interpolation" and rows[at(0)].energy_kwh == 0.4


def test_gap_open_at_the_end_is_not_interpolated(spark):
        rows = filled_by_time(spark, day_of("M1", DAY, skip={46, 47}))
        assert rows[at(23, 30)].estimation_method != "interpolation"


# ---------- T-34.3: profile of the same weekday over the last 4 weeks ----------

def four_mondays(kwh_by_week=("0.1", "0.2", "0.3", "0.4")):
    """The 4 Mondays before DAY, each with its own value."""
    readings = []
    for weeks, kwh in zip((4, 3, 2, 1), kwh_by_week):
        readings += day_of("M1", DAY - timedelta(weeks=weeks), kwh=kwh)
    return readings


def test_5_hour_gap_takes_the_average_of_the_same_time_on_the_last_4_mondays(spark):
    gap = set(range(30, 40))  # 15:00 to 19:30
    rows = filled_by_time(spark, four_mondays() + day_of("M1", DAY, kwh="0.9", skip=gap))
    estimated = [rows[at(15) + i * HALF_HOUR] for i in range(10)]
    assert {r.estimation_method for r in estimated} == {"profile"}
    assert {r.energy_kwh for r in estimated} == {0.25}  # (0.1 + 0.2 + 0.3 + 0.4) / 4
    assert sum(r.is_estimated for r in rows.values()) == 10


def test_profile_uses_only_the_weeks_that_have_a_valid_reading(spark):
    history = four_mondays(("0.1", "Null", "0.3", "-1"))  # two weeks flagged at every interval
    rows = filled_by_time(spark, history + day_of("M1", DAY, skip=set(range(20, 30))))
    assert rows[at(10)].estimation_method == "profile" and rows[at(10)].energy_kwh == 0.2


def test_other_weekdays_do_not_count(spark):
    sunday = day_of("M1", DAY - timedelta(days=1), kwh="5.0")
    rows = filled_by_time(spark, four_mondays() + sunday + day_of("M1", DAY, skip=set(range(20, 30))))
    assert rows[at(12)].energy_kwh == 0.25


def test_gap_without_any_history_is_marked_unavailable(spark):
    rows = filled_by_time(spark, day_of("M1", DAY, skip=set(range(20, 30))))
    row = rows[at(12)]
    assert (row.energy_kwh, row.is_estimated, row.estimation_method) == (None, True, "unavailable")


def test_ca_03_counts_on_the_job(spark):
    gaps = {10, 11, 12} | set(range(30, 40))
    rows = filled_by_time(spark, four_mondays() + day_of("M1", DAY, kwh="0.3", skip=gaps))
    methods = [r.estimation_method for r in rows.values() if r.is_estimated]
    assert methods.count("interpolation") == 3 and methods.count("profile") == 10
