"""RN-08 (declaring a demand-response event) as plain Python, the same thresholds the Spark job uses.
Spark: jobs/dr_events.py, which imports `needs_event`, `event_window` and the thresholds.
"""

from datetime import date, datetime, time, timedelta

TEMPERATURE_THRESHOLD_C = 30.0      # RN-08: forecast temperature above 30 °C ...
LOAD_THRESHOLD = 0.85               # ... and projected load above 85% of the circuit capacity
MAX_DURATION = timedelta(hours=3)   # RN-08: an event lasts 3 hours at most ...
PEAK_START = time(18, 0)            # ... and only inside the peak band (F7: 18:00 to 21:59)
PEAK_END = time(22, 0)


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
