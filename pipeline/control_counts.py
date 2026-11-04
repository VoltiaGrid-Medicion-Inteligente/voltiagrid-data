"""CA-08: a 24-hour resend of 1,000 meters is processed without losing or duplicating a reading.
Spark: jobs/control_counts.py (the same counts on real raw and clean data).
"""

import random
from datetime import datetime, timedelta

from pipeline.cleaning import deduplicate

HALF_HOUR = timedelta(minutes=30)
RESEND_START = datetime(2013, 1, 1)


def resend(meters: int, hours: int, start: datetime = RESEND_START, redelivered: float = 0.05,
           seed: int = 42) -> list[dict]:
    """What arrives after an outage: every reading of every meter for `hours`, in any order, and a share of them twice
    (RabbitMQ delivers at least once: a consumer crash before the ACK stores a message again)."""
    rng = random.Random(seed)
    readings = [{"meter_id": f"MAC{m:06d}", "ts": start + i * HALF_HOUR, "energy_kwh": 0.25, "seq": m * 1000 + i + 1}
                for m in range(meters) for i in range(hours * 2)]
    again = [dict(r) for r in readings if rng.random() < redelivered]
    arrived = readings + again
    rng.shuffle(arrived)
    return arrived


def reconcile(meters: int, hours: int, processed: list[dict] | None = None, start: datetime = RESEND_START) -> dict:
    """Control counts: expected readings vs what the pipeline kept.

    processed: readings after cleaning (meter_id, ts, ...). If not given, a resend is simulated and cleaned with RN-02.
    lost = expected meter-intervals missing; duplicated = kept rows beyond one per meter-interval.
    """
    if processed is None:
        processed = deduplicate(resend(meters, hours, start))
    expected = {(f"MAC{m:06d}", start + i * HALF_HOUR) for m in range(meters) for i in range(hours * 2)}
    kept = [(r["meter_id"], r["ts"]) for r in processed]
    unique = set(kept)
    return {
        "expected": len(expected),
        "processed": len(kept),
        "lost": len(expected - unique),
        "duplicated": len(kept) - len(unique),
        "unexpected": len(unique - expected),
    }
