"""pipeline.demand_response beyond the CA-06 scenario (RN-08)."""

from datetime import date, datetime, time

from pipeline.demand_response import event_window, needs_event

DAY = date(2013, 7, 22)


def test_both_conditions_strictly_above_the_thresholds():
    assert needs_event(30.1, 0.851) is True
    assert needs_event(30.0, 0.95) is False      # 30 °C is not above 30 °C
    assert needs_event(35.0, 0.85) is False      # 85% is not above 85%
    assert needs_event(None, 0.95) is False      # no forecast, no event
    assert needs_event(35.0, None) is False


def test_event_starts_at_the_first_overloaded_interval_and_lasts_3_hours_at_most():
    assert event_window(DAY) == (datetime(2013, 7, 22, 18, 0), datetime(2013, 7, 22, 21, 0))
    assert event_window(DAY, time(19, 30)) == (datetime(2013, 7, 22, 19, 30), datetime(2013, 7, 22, 22, 0))
    assert event_window(DAY, time(21, 30)) == (datetime(2013, 7, 22, 21, 30), datetime(2013, 7, 22, 22, 0))


def test_event_never_starts_before_the_peak_band():
    start, end = event_window(DAY, time(16, 0))
    assert (start.time(), end.time()) == (time(18, 0), time(21, 0))
