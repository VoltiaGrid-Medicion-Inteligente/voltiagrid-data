"""RN-06 (suspicious transformer) and RN-07 (suspicious meter).
Spark: jobs/transformer_losses.py and jobs/suspicious_meters.py, which import these thresholds.
"""

LOSS_THRESHOLD = 0.12          # RN-06: loss above 12% ...
CONSECUTIVE_DAYS = 3           # ... for 3 consecutive days
ZERO_STREAK_INTERVALS = 96     # RN-07: 48 hours of 30-minute readings at 0 kWh
DROP_THRESHOLD = 0.60          # RN-07: drop above 60% versus the 8-week average


def is_suspicious_transformer(daily_loss_pct: list[float | None]) -> bool:
    """RN-06: loss above 12% on 3 consecutive days (a day without a value breaks the streak)."""
    streak = 0
    for pct in daily_loss_pct:
        streak = streak + 1 if pct is not None and pct > LOSS_THRESHOLD else 0
        if streak >= CONSECUTIVE_DAYS:
            return True
    return False


def is_suspicious_meter(zero_streak_intervals: int = 0, drop_pct: float | None = None,
                        contract_changed: bool = False) -> bool:
    """RN-07, for a meter of a suspicious transformer: 48 h of zeros, or a drop above 60% without a contract change."""
    zero_48h = zero_streak_intervals >= ZERO_STREAK_INTERVALS
    drop_60 = drop_pct is not None and drop_pct > DROP_THRESHOLD and not contract_changed
    return zero_48h or drop_60


def evaluate_transformer(daily_loss_pct: list[float | None], meters: list[dict] | None = None) -> dict:
    """RN-06 then RN-07: the meters are evaluated only when the transformer is suspicious.

    meters: dicts with meter_id and optionally zero_streak_intervals, drop_pct, contract_changed.
    """
    suspicious = is_suspicious_transformer(daily_loss_pct)
    flagged = []
    if suspicious:
        flagged = [m["meter_id"] for m in meters or []
                   if is_suspicious_meter(m.get("zero_streak_intervals", 0), m.get("drop_pct"),
                                          m.get("contract_changed", False))]
    return {"is_suspicious": suspicious, "meters_evaluated": suspicious, "suspicious_meters": flagged}
