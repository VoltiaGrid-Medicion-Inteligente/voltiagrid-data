"""pipeline.demand_response beyond the CA-06 scenario (RN-08)."""

from datetime import date, datetime, time

from pipeline.demand_response import declare_event, event_window, needs_event

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


def test_declared_event_keeps_its_inputs():
    event = declare_event(31.5, 0.9, DAY, time(18, 30))
    assert event == {"start": datetime(2013, 7, 22, 18, 30), "end": datetime(2013, 7, 22, 21, 30),
                     "forecast_temp_c": 31.5, "projected_load_pct": 0.9}


# ---------- RN-09: baseline (T-44.1) ----------

def test_baseline_is_the_last_5_working_days_without_an_event():
    from pipeline.demand_response import baseline_days

    assert baseline_days(DAY) == [date(2013, 7, 19), date(2013, 7, 18), date(2013, 7, 17), date(2013, 7, 16),
                                  date(2013, 7, 15)]
    with_event = baseline_days(DAY, {date(2013, 7, 17)})
    assert with_event == [date(2013, 7, 19), date(2013, 7, 18), date(2013, 7, 16), date(2013, 7, 15),
                          date(2013, 7, 12)]                     # skips the 17th and the weekend


def test_baseline_needs_the_5_days():
    from pipeline.demand_response import baseline_kwh

    days = {date(2013, 7, d): 6.0 for d in (15, 16, 17, 18)}
    assert baseline_kwh(days) is None
    days[date(2013, 7, 19)] = 12.0
    assert baseline_kwh(days) == 7.2


# ---------- RN-09: saving and incentive (T-44.2) ----------

def test_saving_in_kwh_and_share_of_the_baseline():
    from pipeline.demand_response import savings

    assert savings(12.0, 10.8) == (1.2, 0.1)
    assert savings(7.2, 8.0) == (-0.8, -0.1111)    # consumed more than usual: a negative saving
    assert savings(None, 5.0) == (None, None)


def test_incentive_is_each_kwh_saved_at_the_peak_price_from_10_percent():
    from pipeline.demand_response import incentive_amount

    assert incentive_amount(1.2, 0.1, 0.30) == 0.36
    assert incentive_amount(0.96, 0.08, 0.30) == 0.0
    assert incentive_amount(None, None, 0.30) == 0.0
