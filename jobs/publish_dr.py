"""Publish the settled demand-response events to the voltiagrid-api database (US-44, T-44.4).

    python -m jobs.publish_dr --date 2013-07-22 --lake ../voltiagrid-api/lake      # DATABASE_URL: environment or .env
    python -m jobs.publish_dr --run f3-2026-11-02 --lake ../voltiagrid-api/lake

The customer consumption API answers `GET /me/events` from the table `dr_participation` (migration in voltiagrid-api).
This job copies there, for the event days given, one row per event and customer from curated dr_settlement with the
event window from dr_events. It replaces the rows of those days in one transaction, so running it twice changes nothing.
"""

import argparse
import os
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.dr_events import read_events
from jobs.dr_settlement import settlement_path
from jobs.estimate_gaps import as_dates
from jobs.inventory import _from_dotenv, psycopg_url
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, path_exists
from jobs.spark import get_spark

COLUMNS = ["event_id", "customer_code", "event_date", "start_ts", "end_ts", "baseline_kwh", "actual_kwh",
           "savings_kwh", "savings_pct", "incentive_paid", "incentive_amount"]
# Only the columns published, with their types: a lake without settled events (no parquet file yet) still reads.
SETTLEMENT_SCHEMA = ("event_id string, customer_code string, baseline_kwh double, actual_kwh double, savings_kwh double, "
                     "savings_pct double, incentive_paid boolean, incentive_amount double, date date")
DELETE_SQL = "DELETE FROM dr_participation WHERE event_date = ANY(%s)"
INSERT_SQL = f"INSERT INTO dr_participation ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))})"


def participation_rows(settlement: DataFrame, events: DataFrame) -> list[tuple]:
    """Rows in COLUMNS order. The window is stored in UTC: the API returns it with its time zone."""
    joined = settlement.join(events.select("event_id", "start_ts", "end_ts"), "event_id")
    return [tuple(r) for r in joined.select(
        "event_id", "customer_code", F.col("date").alias("event_date"), "start_ts", "end_ts", "baseline_kwh",
        "actual_kwh", "savings_kwh", "savings_pct", "incentive_paid", "incentive_amount",
    ).orderBy("event_date", "event_id", "customer_code").collect()]


def publish(connection, rows: list[tuple], days: list[date]) -> int:
    """Replace the rows of these days. `connection` is a psycopg connection (or a fake one in the tests)."""
    with connection.cursor() as cur:
        cur.execute(DELETE_SQL, (days,))
        if rows:
            cur.executemany(INSERT_SQL, rows)
    connection.commit()
    return len(rows)


def run(spark: SparkSession, lake: Lake, days: list, connection) -> int:
    days = as_dates(days)
    path = settlement_path(lake)
    if not path_exists(spark, path):          # nothing settled yet: the days are only cleared
        return publish(connection, [], days)
    settlement = spark.read.schema(SETTLEMENT_SCHEMA).parquet(path).filter(F.col("date").isin(days))
    events = read_events(spark, lake).filter(F.col("date").isin(days))
    return publish(connection, participation_rows(settlement, events), days)


def main() -> None:
    import psycopg

    p = argparse.ArgumentParser(description="Publish demand-response participation to the API database (US-44)")
    add_lake_arguments(p)
    add_date_arguments(p)
    p.add_argument("--database-url", default=None, help="default: DATABASE_URL (environment or .env)")
    args = p.parse_args()
    url = args.database_url or os.getenv("DATABASE_URL") or _from_dotenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is missing: the same value as in voltiagrid-api/.env (localhost:5433)")
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-publish-dr")
    days = dates_from_args(spark, args, lake)
    with psycopg.connect(psycopg_url(url)) as connection:
        count = run(spark, lake, days, connection)
    print(f"publish_dr: {count} customer-events of {len(days)} dates in dr_participation")
    spark.stop()


if __name__ == "__main__":
    main()
