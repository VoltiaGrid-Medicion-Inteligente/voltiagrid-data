"""Settlement of the demand-response events of some days (US-44): baseline (T-44.1), saving and incentive (T-44.2).

    python -m jobs.dr_settlement --date 2013-07-22 --lake ../voltiagrid-api/lake
    python -m jobs.dr_settlement --run f3-2026-11-02 --lake ../voltiagrid-api/lake

Runs after jobs.dr_events and jobs.estimate_gaps for the event day and the weeks before it. For each event and
participating customer: the baseline days are the last 5 working days before the event on which that customer had no
event (pipeline.demand_response.baseline_days, RN-09), and the baseline is the average energy of the customer's meters
in the event window (same clock times) over those days. Real and estimated readings both count (filled, US-34): a gap
must not look like a saving. Without 5 baseline days there is no fair baseline (baseline_kwh null).

Saving (T-44.2): actual = the same window on the event day; saving = baseline - actual, in kWh and in % of the
baseline. From 10% on the customer gets the incentive: each kWh saved at the F7 peak price of the day (RN-09).

Output: curated/dataset=dr_settlement/date=YYYY-MM-DD/, one row per event and customer.
"""

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.dr_events import read_events, read_participants
from jobs.estimate_gaps import as_dates, filled_path
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from jobs.tariff_bands import DEFAULT_TARIFFS, read_tariffs
from pipeline.demand_response import BASELINE_DAYS, INCENTIVE_MIN_SAVING, baseline_days

PLAN_SCHEMA = "event_id string, customer_code string, baseline_date date"


def settlement_path(lake: Lake) -> str:
    return lake.curated("dr_settlement")


def baseline_plan(spark: SparkSession, events: list, participants: DataFrame, all_participants: DataFrame) -> DataFrame:
    """(event_id, customer_code, baseline_date): which days are the baseline of each participant of each event.

    A customer's event days come from every participation in the lake, so a day with an earlier event is skipped."""
    event_days = {}
    for r in all_participants.select("customer_code", "date").distinct().collect():
        event_days.setdefault(r.customer_code, set()).add(r.date)
    rows = []
    for e in events:
        customers = [r.customer_code for r in participants.filter(F.col("event_id") == e.event_id)
                     .select("customer_code").distinct().collect()]
        for customer in sorted(customers):
            rows += [(e.event_id, customer, d) for d in baseline_days(e.date, event_days.get(customer, set()))]
    return spark.createDataFrame(rows, PLAN_SCHEMA)


def window_energy(filled: DataFrame, day_by_customer: DataFrame, participants: DataFrame, events: DataFrame) -> DataFrame:
    """kWh of each customer's meters inside the event window (same clock times) on each requested day.

    day_by_customer: (event_id, customer_code, day). Columns: event_id, customer_code, day, kwh, intervals."""
    window = events.select("event_id", F.date_format("start_ts", "HH:mm:ss").alias("_from"),
                           F.date_format("end_ts", "HH:mm:ss").alias("_to"))
    meters = participants.select("event_id", "customer_code", "meter_id").distinct()
    clock = F.date_format("interval_start", "HH:mm:ss")
    return (
        day_by_customer.join(meters, ["event_id", "customer_code"]).join(window, "event_id")
        .join(filled.select("meter_id", "interval_start", "energy_kwh", F.col("date").alias("day")), ["meter_id", "day"])
        .filter((clock >= F.col("_from")) & (clock < F.col("_to")))
        .groupBy("event_id", "customer_code", "day")
        .agg(F.sum("energy_kwh").alias("kwh"), F.count("*").alias("intervals"))
    )


def baselines(spark: SparkSession, filled: DataFrame, events: DataFrame, participants: DataFrame,
              all_participants: DataFrame) -> DataFrame:
    """event_id, customer_code, baseline_days, baseline_kwh (null without 5 days with readings), date."""
    plan = baseline_plan(spark, events.collect(), participants, all_participants)
    per_day = window_energy(filled, plan.withColumnRenamed("baseline_date", "day"), participants, events)
    stats = per_day.groupBy("event_id", "customer_code").agg(F.count("*").alias("baseline_days"),
                                                             F.avg("kwh").alias("_avg"))
    return (
        participants.select("event_id", "customer_code", "date").distinct()
        .join(stats, ["event_id", "customer_code"], "left")
        .withColumn("baseline_days", F.coalesce("baseline_days", F.lit(0)))
        .withColumn("baseline_kwh", F.when(F.col("baseline_days") >= BASELINE_DAYS, F.round("_avg", 4)))
        .select("event_id", "customer_code", "baseline_days", "baseline_kwh", "date")
    )


def peak_price(tariffs: DataFrame) -> DataFrame:
    """(date range, price): the F7 peak price, to value the kWh saved inside the event window (always peak band)."""
    return tariffs.filter(F.col("band_name") == "peak").select("valid_from", "valid_to", "price_per_kwh")


def settle(spark: SparkSession, filled: DataFrame, events: DataFrame, participants: DataFrame,
           all_participants: DataFrame, tariffs: DataFrame) -> DataFrame:
    """participants: the ones of these events; all_participants: every participation (to skip earlier event days)."""
    base = baselines(spark, filled, events, participants, all_participants)
    event_day = participants.select("event_id", "customer_code", F.col("date").alias("day")).distinct()
    actual = window_energy(filled, event_day, participants, events).select(
        "event_id", "customer_code", F.round("kwh", 4).alias("actual_kwh"))
    saved = F.col("baseline_kwh") - F.col("actual_kwh")
    pct = F.round(saved / F.col("baseline_kwh"), 4)
    paid = F.coalesce(pct >= F.lit(INCENTIVE_MIN_SAVING), F.lit(False))
    price = F.col("price_per_kwh").cast("double")
    return (
        base.join(actual, ["event_id", "customer_code"], "left")
        .join(F.broadcast(peak_price(tariffs)), F.col("date").between(F.col("valid_from"), F.col("valid_to")), "left")
        .withColumn("savings_kwh", F.round(saved, 4))
        .withColumn("savings_pct", pct)
        .withColumn("incentive_paid", paid)
        .withColumn("incentive_amount", F.when(paid, F.round(F.col("savings_kwh") * price, 2)).otherwise(F.lit(0.0)))
        .select("event_id", "customer_code", "baseline_days", "baseline_kwh", "actual_kwh", "savings_kwh",
                "savings_pct", "incentive_paid", price.alias("price_per_kwh"), "incentive_amount", "date")
    )


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    events = read_events(spark, lake).filter(F.col("date").isin(days)).localCheckpoint(eager=True)
    all_participants = read_participants(spark, lake)
    participants = all_participants.join(events.select("event_id"), "event_id").localCheckpoint(eager=True)
    filled = spark.read.parquet(filled_path(lake))
    tariffs = read_tariffs(spark, DEFAULT_TARIFFS)
    result = settle(spark, filled, events, participants, all_participants, tariffs).localCheckpoint(eager=True)
    _write(spark, result, settlement_path(lake), days)
    return result


def _write(spark: SparkSession, df: DataFrame, path: str, days: list[date]) -> None:
    df.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")


def main() -> None:
    p = argparse.ArgumentParser(description="Settlement of demand-response events (US-44)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-dr-settlement")
    days = dates_from_args(spark, args, lake)
    rows = run(spark, lake, days).orderBy("event_id", "customer_code").collect()
    print(f"dr_settlement: {len(rows)} customer-events in {len(days)} dates")
    for r in rows:
        if r.baseline_kwh is None:
            print(f"  {r.event_id} {r.customer_code}: no baseline ({r.baseline_days} days)")
            continue
        print(f"  {r.event_id} {r.customer_code}: baseline {r.baseline_kwh:.3f} kWh, actual {r.actual_kwh:.3f}, "
              f"saving {r.savings_pct:.1%}, incentive {r.incentive_amount:.2f}")
    spark.stop()


if __name__ == "__main__":
    main()
