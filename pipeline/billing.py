"""RN-04: a customer is not billed with more than 10% estimated readings in the month. Spark: jobs/billing.py."""

MAX_ESTIMATED_PCT = 0.10
REASON = "estimated readings above 10% of the month (RN-04)"


def close_month(estimated_pct: float) -> dict:
    """Decision for one customer and month. estimated_pct = estimated intervals / intervals (0.12 = 12%)."""
    blocked = estimated_pct > MAX_ESTIMATED_PCT
    return {
        "estimated_pct": estimated_pct,
        "inspection_order": blocked,
        "is_billable": not blocked,
        "reason": REASON if blocked else None,
    }
