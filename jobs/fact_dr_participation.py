"""Demand-response fact of the analytics layer (US-45, T-45.1): one row per customer and event.

    python -m jobs.fact_dr_participation --date 2013-07-22 --lake ../voltiagrid-api/lake
    python -m jobs.fact_dr_participation --run f3-2026-11-02 --lake ../voltiagrid-api/lake

Grain: one customer in one demand-response event (dimensional model, fact_dr_participation). Runs after
jobs.dr_settlement. Measures: baseline_kwh, actual_kwh, savings_kwh, savings_pct, incentive_paid (0/1) and
incentive_amount. Keys: date_key, event_id (dr_events), customer_code and circuit_code (F4).
A customer without a fair baseline (fewer than 5 days, RN-09) keeps its row with null measures and incentive 0: it
took part, it just could not be settled.

Output: curated/dataset=fact_dr_participation/date=YYYY-MM-DD/
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.analytics import check_no_orphans, date_key
from jobs.dr_events import read_events
from jobs.dr_settlement import settlement_path
from jobs.estimate_gaps import as_dates
from jobs.inventory import read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark

SETTLEMENT_SCHEMA = ("event_id string, customer_code string, baseline_days int, baseline_kwh double, "
                     "actual_kwh double, savings_kwh double, savings_pct double, incentive_paid boolean, "
                     "price_per_kwh double, incentive_amount double, date date")
FACT_COLUMNS = ["date_key", "event_id", "customer_code", "circuit_code", "baseline_kwh", "actual_kwh", "savings_kwh",
                "savings_pct", "incentive_paid", "incentive_amount", "date"]


def participation_fact(settlement: DataFrame, events: DataFrame) -> DataFrame:
    return (
        settlement.join(events.select("event_id", "circuit_code"), "event_id", "left")
        .withColumn("date_key", date_key("date"))
        .withColumn("incentive_paid", F.col("incentive_paid").cast("int"))
        .select(FACT_COLUMNS)
    )


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    settlement = spark.read.schema(SETTLEMENT_SCHEMA).parquet(settlement_path(lake)).filter(F.col("date").isin(days))
    events = read_events(spark, lake)
    fact = participation_fact(settlement, events).localCheckpoint(eager=True)
    check_no_orphans(fact, {"event_id": events, "customer_code": read_meters(spark, lake)}, "fact_dr_participation")
    path = lake.curated("fact_dr_participation")
    fact.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")
    return fact


def main() -> None:
    p = argparse.ArgumentParser(description="Demand-response fact per customer and event (US-45)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-fact-dr-participation")
    days = dates_from_args(spark, args, lake)
    s = run(spark, lake, days).agg(F.count("*").alias("rows"), F.countDistinct("event_id").alias("events"),
                                   F.sum("savings_kwh").alias("saved"), F.sum("incentive_amount").alias("paid")).first()
    print(f"fact_dr_participation: {s.rows} customer-events ({s.events} events) in {len(days)} dates, "
          f"{s.saved or 0:,.1f} kWh saved, {s.paid or 0:,.2f} paid in incentives; no orphan keys")
    spark.stop()


if __name__ == "__main__":
    main()
