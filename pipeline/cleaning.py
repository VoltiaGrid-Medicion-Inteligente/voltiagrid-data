"""RN-01 (clock) and RN-02 (duplicates). Spark version: jobs/clean_readings.py."""

from datetime import datetime, timedelta

HALF_HOUR = timedelta(minutes=30)
TOLERANCE = timedelta(minutes=5)


def normalize_clock(ts: datetime) -> tuple[datetime | None, bool]:
    """RN-01: (nearest :00 or :30, False) if ts is at most 5 minutes from it; (None, True) = invalid clock."""
    floor = ts.replace(minute=0, second=0, microsecond=0) + HALF_HOUR * (ts.minute // 30)
    offset = ts - floor
    if offset <= TOLERANCE:
        return floor, False
    if offset >= HALF_HOUR - TOLERANCE:
        return floor + HALF_HOUR, False
    return None, True


def deduplicate(readings: list[dict]) -> list[dict]:
    """RN-02: per meter and normalized interval, keep the reading with the highest sequence.

    readings: dicts with meter_id, ts, energy_kwh, seq. Exact copies count once. Readings with an invalid clock have
    no interval and are returned unchanged (the cleaning job flags them).
    """
    best: dict[tuple, dict] = {}
    invalid = []
    for r in readings:
        interval, invalid_clock = normalize_clock(r["ts"])
        if invalid_clock:
            if r not in invalid:
                invalid.append(r)
            continue
        key = (r["meter_id"], interval)
        if key not in best or r["seq"] > best[key]["seq"]:
            best[key] = r
    return list(best.values()) + invalid
