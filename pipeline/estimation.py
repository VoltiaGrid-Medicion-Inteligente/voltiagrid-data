"""RN-03: estimate missing readings. Spark version: jobs/estimate_gaps.py."""

from collections import defaultdict
from datetime import timedelta

HALF_HOUR = timedelta(minutes=30)
MAX_INTERPOLATED = 4   # intervals: gaps up to 2 hours
PROFILE_WEEKS = 4


def _profile(actual: dict, ts) -> float | None:
    """Average of the same time on the same weekday 1 to 4 weeks before, real readings only."""
    values = [actual[ts - timedelta(weeks=k)] for k in range(1, PROFILE_WEEKS + 1) if ts - timedelta(weeks=k) in actual]
    return round(sum(values) / len(values), 4) if values else None


def fill_gaps(series: list[dict]) -> list[dict]:
    """series: readings (meter_id, ts, energy_kwh, seq); energy_kwh None = invalid. Returns one row per meter and
    30-minute interval from its first to its last reading, with is_estimated and estimation_method:
    'interpolation' (gap up to 2 h with both neighbours), 'profile' (4-week same weekday), 'unavailable', or None.
    """
    by_meter: dict[str, dict] = defaultdict(dict)
    for r in series:
        if r["energy_kwh"] is not None:
            by_meter[r["meter_id"]][r["ts"]] = r["energy_kwh"]
        else:
            by_meter[r["meter_id"]].setdefault(r["ts"], None)

    result = []
    for meter, readings in by_meter.items():
        actual = {ts: kwh for ts, kwh in readings.items() if kwh is not None}
        first, last = min(readings), max(readings)
        grid = [first + i * HALF_HOUR for i in range(int((last - first) / HALF_HOUR) + 1)]
        i = 0
        while i < len(grid):
            ts = grid[i]
            if ts in actual:
                result.append({"meter_id": meter, "ts": ts, "energy_kwh": actual[ts],
                               "is_estimated": False, "estimation_method": None})
                i += 1
                continue
            j = i
            while j + 1 < len(grid) and grid[j + 1] not in actual:
                j += 1
            prev_ts = grid[i - 1] if i > 0 else None
            next_ts = grid[j + 1] if j + 1 < len(grid) else None
            length = j - i + 1
            for k in range(i, j + 1):
                gap_ts = grid[k]
                if prev_ts is not None and next_ts is not None and length <= MAX_INTERPOLATED:
                    step = (actual[next_ts] - actual[prev_ts]) / (length + 1)
                    value, method = round(actual[prev_ts] + step * (k - i + 1), 4), "interpolation"
                else:
                    value = _profile(actual, gap_ts)
                    method = "profile" if value is not None else "unavailable"
                result.append({"meter_id": meter, "ts": gap_ts, "energy_kwh": value,
                               "is_estimated": True, "estimation_method": method})
            i = j + 1
    return result
