"""RN-08 (declaring a demand-response event), RN-09 (settling it) and RN-10 (monthly limit), the rules the Spark jobs use.
Spark: jobs/dr_events.py (needs_event, event_window, MAX_EVENTS_PER_MONTH) and jobs/dr_settlement.py
(baseline_days, INCENTIVE_MIN_SAVING).
"""

from datetime import date, datetime, time, timedelta

TEMPERATURE_THRESHOLD_C = 30.0      # RN-08: forecast temperature above 30 °C ...
LOAD_THRESHOLD = 0.85               # ... and projected load above 85% of the circuit capacity
MAX_DURATION = timedelta(hours=3)   # RN-08: an event lasts 3 hours at most ...
PEAK_START = time(18, 0)            # ... and only inside the peak band (F7: 18:00 to 21:59)
PEAK_END = time(22, 0)
BASELINE_DAYS = 5                   # RN-09: the same intervals of the previous 5 working days without an event
BASELINE_LOOKBACK_DAYS = 28         # how far back to look for them (4 weeks of working days is plenty)
INCENTIVE_MIN_SAVING = 0.10         # RN-09: incentive only with a saving of 10% or more
MAX_EVENTS_PER_MONTH = 8            # RN-10: a customer takes part in 8 events a month at most


def needs_event(forecast_temp_c: float | None, projected_load_pct: float | None) -> bool:
    """RN-08: both conditions, strictly above the thresholds. A missing value never declares an event."""
    if forecast_temp_c is None or projected_load_pct is None:
        return False
    return forecast_temp_c > TEMPERATURE_THRESHOLD_C and projected_load_pct > LOAD_THRESHOLD


def event_window(day: date, overload_from: time = PEAK_START) -> tuple[datetime, datetime]:
    """Start at the first overloaded peak interval (never before 18:00), end 3 hours later or at 22:00."""
    start = datetime.combine(day, max(overload_from, PEAK_START))
    end = min(start + MAX_DURATION, datetime.combine(day, PEAK_END))
    return start, end


def declare_event(forecast_temp_c: float | None, projected_load_pct: float | None, day: date | None = None,
                  overload_from: time = PEAK_START) -> dict | None:
    """RN-08 / CA-06: the event (start, end and the two inputs) when both conditions hold; None otherwise."""
    if not needs_event(forecast_temp_c, projected_load_pct):
        return None
    start, end = event_window(day or date.today(), overload_from)
    return {"start": start, "end": end, "forecast_temp_c": forecast_temp_c, "projected_load_pct": projected_load_pct}


def is_working_day(day: date) -> bool:
    """Monday to Friday. The pilot has no holiday calendar (an open point for the client)."""
    return day.weekday() < 5


def baseline_days(event_day: date, event_days: set[date] | frozenset = frozenset()) -> list[date]:
    """RN-09: the last 5 working days before the event on which the customer had no event, most recent first.
    Fewer than 5 inside the 4-week lookback -> the list is shorter and there is no fair baseline."""
    days = []
    for back in range(1, BASELINE_LOOKBACK_DAYS + 1):
        day = event_day - timedelta(days=back)
        if is_working_day(day) and day not in event_days:
            days.append(day)
            if len(days) == BASELINE_DAYS:
                break
    return days


def baseline_kwh(kwh_by_day: dict[date, float]) -> float | None:
    """Average energy of the event intervals over the baseline days; None without the 5 days."""
    if len(kwh_by_day) < BASELINE_DAYS:
        return None
    return sum(kwh_by_day.values()) / len(kwh_by_day)


def savings(baseline: float | None, actual: float) -> tuple[float | None, float | None]:
    """RN-09: saving = baseline - actual, in kWh and as a share of the baseline (rounded to 4 decimals)."""
    if not baseline:
        return None, None
    saved = baseline - actual
    return round(saved, 4), round(saved / baseline, 4)


def incentive_amount(savings_kwh: float | None, savings_pct: float | None, price_per_kwh: float) -> float:
    """Incentive paid: each kWh saved at the peak price of the day (F7), only from a 10% saving on."""
    if savings_pct is None or savings_pct < INCENTIVE_MIN_SAVING:
        return 0.0
    return round(savings_kwh * price_per_kwh, 2)
