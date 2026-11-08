"""Loss-injection scenario (US-62, T-62.1): inject losses in F5 for chosen transformers, run detection, check the result.

    python -m jobs.loss_injection_scenario --lake ../voltiagrid-api/lake --from 2013-01-17 --days 3 \
        --transformers TR-00001 --loss-pct 0.15

Steps, in order:
1. POST /losses on F5: one injection per transformer for the period (F5 delivers the meters' energy x (1 + loss)).
2. Fetch F5 to raw for the days (jobs.fetch_f5), then jobs.transformer_losses and jobs.suspicious_meters.
3. Check: every injected transformer is suspicious on the last day of the period (RN-06), and each of its meters was
   evaluated (RN-07) on that day.
4. Remove the injections. F5 keeps them in memory, so they are removed also when a step above fails.

The filled readings (US-34) and F4 must already exist in the lake for the days. Exit code 1 when the check fails.
"""

import argparse
import json
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.fetch_f5 import fetch_day, http_get
from jobs.lake import Lake, add_lake_arguments, lake_from_args
from jobs.spark import get_spark


def injection_body(transformer: str, start: date, days: int, loss_pct: float) -> dict:
    """The body of POST /losses: start and end are timezone-aware ISO 8601 (F5 contract)."""
    begin = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end = begin + timedelta(days=days)
    return {"transformer_code": transformer, "start": begin.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"), "loss_pct": loss_pct}


def http_post(url: str, body: dict) -> dict:
    request = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def http_delete(url: str) -> None:
    request = urllib.request.Request(url, method="DELETE")
    with urllib.request.urlopen(request, timeout=60):
        pass


def inject(base_url: str, body: dict, post: Callable[[str, dict], dict] = http_post) -> int:
    """Returns the injection id that F5 assigned."""
    return int(post(f"{base_url}/losses", body)["id"])


def remove(base_url: str, injection_id: int, delete: Callable[[str], None] = http_delete) -> None:
    delete(f"{base_url}/losses/{injection_id}")


def check_scenario(suspicious: DataFrame, evaluated: DataFrame, transformers: list[str], last_day: date) -> list[str]:
    """Problems found, one line each; an empty list means the scenario held.

    suspicious: suspicious_transformers rows (transformer_code, date); evaluated: suspicious_meters rows
    (meter_id, transformer_code, date).
    """
    problems = []
    flagged = {r.transformer_code for r in suspicious.filter(F.col("date") == F.lit(last_day)).collect()}
    checked = evaluated.filter(F.col("date") == F.lit(last_day))
    for code in transformers:
        if code not in flagged:
            problems.append(f"{code} is not suspicious on {last_day.isoformat()}")
        meters = checked.filter(F.col("transformer_code") == code).count()
        if meters == 0:
            problems.append(f"no meter of {code} was evaluated on {last_day.isoformat()}")
    return problems


def detect(spark: SparkSession, lake: Lake, days: list[str]) -> tuple[DataFrame, DataFrame]:
    """The two detection jobs for the days; returns (suspicious transformers, evaluated meters)."""
    from jobs import suspicious_meters, transformer_losses
    suspicious = transformer_losses.run(spark, lake, days)
    evaluated = suspicious_meters.run(spark, lake, days)
    return suspicious, evaluated


def run_scenario(spark: SparkSession, lake: Lake, f5_url: str, transformers: list[str], start: date, days: int,
                 loss_pct: float, *, post=http_post, delete=http_delete, get=http_get,
                 detect_fn=detect) -> list[str]:
    period = [(start + timedelta(days=i)).isoformat() for i in range(days)]
    ids = []
    try:
        for code in transformers:
            ids.append(inject(f5_url, injection_body(code, start, days, loss_pct), post=post))
        today = datetime.now(timezone.utc).date()
        for d in period:
            fetch_day(lake, f5_url, date.fromisoformat(d), today, get=get)
        suspicious, evaluated = detect_fn(spark, lake, period)
        return check_scenario(suspicious, evaluated, transformers, date.fromisoformat(period[-1]))
    finally:
        for injection_id in ids:
            remove(f5_url, injection_id, delete=delete)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_lake_arguments(p)
    p.add_argument("--from", dest="start", required=True, help="first day of the injection (YYYY-MM-DD)")
    p.add_argument("--days", type=int, default=3, help="days of injected loss (RN-06 needs 3 or more)")
    p.add_argument("--transformers", required=True, help="comma-separated transformer codes, e.g. TR-00001,TR-00002")
    p.add_argument("--loss-pct", type=float, default=0.15, help="injected loss, 0.15 = 15%%")
    p.add_argument("--f5-url", default=os.getenv("F5_URL", "http://localhost:8005"))
    args = p.parse_args()
    if args.days < 3:
        sys.exit("--days must be 3 or more: RN-06 needs three consecutive days above 12%")
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-loss-injection", ui=False)
    transformers = [t.strip() for t in args.transformers.split(",") if t.strip()]
    problems = run_scenario(spark, lake, args.f5_url.rstrip("/"), transformers, date.fromisoformat(args.start),
                            args.days, args.loss_pct)
    spark.stop()
    if problems:
        for line in problems:
            print(f"FAIL {line}")
        sys.exit(1)
    print(f"loss-injection: {', '.join(transformers)} suspicious on the last day, their meters evaluated")


if __name__ == "__main__":
    main()
