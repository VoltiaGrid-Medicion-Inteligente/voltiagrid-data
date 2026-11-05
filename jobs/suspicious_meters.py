"""Suspicious meters of suspicious transformers (US-38, RN-07): zero streaks (T-38.1).

    python -m jobs.suspicious_meters --date 2013-01-17 --lake ../voltiagrid-api/lake

Runs after jobs.transformer_losses. For every transformer that is suspicious on the day (RN-06), its meters (F4) are
evaluated:
- zero consumption for 48 hours or more (96 consecutive real readings of 0 kWh), looking at the day and the two before.
Output: curated/dataset=suspicious_meters/date=YYYY-MM-DD/, one row per evaluated meter with is_suspicious and reason.
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.estimate_gaps import as_dates, filled_path
from jobs.inventory import read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from jobs.transformer_losses import suspicious_path

ZERO_STREAK_INTERVALS = 96     # RN-07: 48 hours of 30-minute readings
ZERO_LOOKBACK_DAYS = 2         # the day and the two before: room for a 48-hour streak that reaches the day


def zero_streaks(filled: DataFrame) -> DataFrame:
    """Longest run of consecutive real zero readings per meter (estimates and missing readings break a run: an
    estimate is not evidence that the meter read zero). Columns: meter_id, zero_streak_intervals.

    Gaps and islands: row_number over all intervals minus row_number over the intervals with the same zero/non-zero
    state is constant inside each run.
    """
    is_zero = (~F.col("is_estimated")) & (F.col("energy_kwh") == 0)
    by_meter = Window.partitionBy("meter_id").orderBy("interval_start")
    by_state = Window.partitionBy("meter_id", "_zero").orderBy("interval_start")
    runs = (
        filled.withColumn("_zero", F.coalesce(is_zero, F.lit(False)))
        .withColumn("_run", F.row_number().over(by_meter) - F.row_number().over(by_state))
        .filter(F.col("_zero"))
        .groupBy("meter_id", "_run").count()
    )
    longest = runs.groupBy("meter_id").agg(F.max("count").alias("zero_streak_intervals"))
    meters = filled.select("meter_id").distinct()
    return meters.join(longest, "meter_id", "left").fillna({"zero_streak_intervals": 0})


def evaluate_meters(meters_to_check: DataFrame, filled_window: DataFrame) -> DataFrame:
    """meters_to_check: meter_id, transformer_code, customer_code. filled_window: filled readings of the lookback."""
    streaks = zero_streaks(filled_window)
    zero_48h = F.col("zero_streak_intervals") >= ZERO_STREAK_INTERVALS
    return (
        meters_to_check.join(streaks, "meter_id", "left")
        .fillna({"zero_streak_intervals": 0})
        .withColumn("zero_48h", zero_48h)
        .withColumn("is_suspicious", zero_48h)
        .withColumn("reason", F.when(zero_48h, F.lit("zero consumption for 48 h or more")))
    )


def meters_of_suspicious_transformers(spark: SparkSession, lake: Lake, day: date) -> DataFrame:
    suspicious = spark.read.parquet(suspicious_path(lake)).filter(F.col("date") == F.lit(day)).select("transformer_code")
    return read_meters(spark, lake).join(suspicious, "transformer_code").select(
        "meter_id", "transformer_code", "customer_code")


def run_day(spark: SparkSession, lake: Lake, day: date) -> DataFrame:
    candidates = meters_of_suspicious_transformers(spark, lake, day)
    filled = spark.read.parquet(filled_path(lake))
    window = filled.filter(F.col("date").between(day - timedelta(days=ZERO_LOOKBACK_DAYS), day))
    return evaluate_meters(candidates, window.join(candidates.select("meter_id"), "meter_id")) \
        .withColumn("date", F.lit(day))


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    result = None
    for day in days:
        part = run_day(spark, lake, day)
        result = part if result is None else result.unionByName(part)
    path = lake.curated("suspicious_meters")
    result = result.localCheckpoint(eager=True)
    result.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")
    return result


def main() -> None:
    p = argparse.ArgumentParser(description="Suspicious meters of suspicious transformers (US-38, RN-07)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-suspicious-meters")
    result = run(spark, lake, dates_from_args(spark, args, lake))
    evaluated = result.count()
    flagged = result.filter("is_suspicious").orderBy("date", "transformer_code", "meter_id").collect()
    print(f"suspicious_meters: {evaluated:,} meters evaluated, {len(flagged)} suspicious")
    for r in flagged:
        print(f"  {r.date} {r.transformer_code} {r.meter_id} ({r.customer_code}): {r.reason}")
    spark.stop()


if __name__ == "__main__":
    main()
