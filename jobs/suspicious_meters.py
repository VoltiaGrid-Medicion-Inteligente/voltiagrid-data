"""Suspicious meters of suspicious transformers (US-38, RN-07): zero streaks (T-38.1), drop vs 8 weeks (T-38.2).

    python -m jobs.suspicious_meters --date 2013-01-17 --lake ../voltiagrid-api/lake

Runs after jobs.transformer_losses. For every transformer that is suspicious on the day (RN-06), its meters (F4) are
evaluated:
- zero consumption for 48 hours or more (96 consecutive real readings of 0 kWh), looking at the day and the two before;
- or consumption of the last 7 days more than 60% below its average of the 8 weeks before, without a contract change.
Output: curated/dataset=suspicious_meters/date=YYYY-MM-DD/, one row per evaluated meter with is_suspicious and reason.
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.estimate_gaps import as_dates, filled_path
from jobs.inventory import read_contracts, read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from jobs.transformer_losses import suspicious_path
from pipeline.losses import DROP_THRESHOLD, ZERO_STREAK_INTERVALS  # RN-07, shared with the rule

ZERO_LOOKBACK_DAYS = 2         # the day and the two before: room for a 48-hour streak that reaches the day
RECENT_DAYS = 7                # "current" consumption: the day and the 6 before
BASELINE_WEEKS = 8             # RN-07: average of the 8 weeks before the recent days
MIN_BASELINE_DAYS = 28         # with less history there is no fair average to compare with


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


def periods(day: date) -> tuple[tuple[date, date], tuple[date, date]]:
    """(recent first, recent last), (baseline first, baseline last). Baseline = the 8 weeks just before the recent days."""
    recent = (day - timedelta(days=RECENT_DAYS - 1), day)
    baseline_last = recent[0] - timedelta(days=1)
    return recent, (baseline_last - timedelta(weeks=BASELINE_WEEKS) + timedelta(days=1), baseline_last)


def consumption_drop(filled: DataFrame, day: date) -> DataFrame:
    """Average daily kWh of the last 7 days vs the 8 weeks before (real and estimated readings: a meter with gaps is
    not a meter that dropped). drop_pct = 1 - recent / baseline; null when the baseline has fewer than 28 days or is 0.
    Columns: meter_id, avg_daily_kwh_8w, baseline_days, avg_daily_kwh_last_week, drop_pct.
    """
    (r_first, r_last), (b_first, b_last) = periods(day)
    daily = filled.filter(F.col("date").between(b_first, r_last)).groupBy("meter_id", "date").agg(
        F.sum("energy_kwh").alias("kwh"))
    in_baseline = F.col("date").between(b_first, b_last)
    in_recent = F.col("date").between(r_first, r_last)
    agg = daily.groupBy("meter_id").agg(
        F.avg(F.when(in_baseline, F.col("kwh"))).alias("avg_daily_kwh_8w"),
        F.count(F.when(in_baseline, True)).alias("baseline_days"),
        F.avg(F.when(in_recent, F.col("kwh"))).alias("avg_daily_kwh_last_week"),
    )
    usable = (F.col("baseline_days") >= MIN_BASELINE_DAYS) & (F.col("avg_daily_kwh_8w") > 0)
    return agg.withColumn(
        "drop_pct", F.when(usable, F.round(1 - F.col("avg_daily_kwh_last_week") / F.col("avg_daily_kwh_8w"), 4)))


def contract_changes(contracts: DataFrame, day: date) -> DataFrame:
    """Customers with a contract that started or ended between the start of the baseline and the day: their drop can
    be explained (new tariff, new holder, move) and does not make the meter suspicious. Columns: customer_code."""
    (_, _), (b_first, _) = periods(day)
    started = F.col("start_date").between(b_first, day)
    ended = F.col("end_date").isNotNull() & F.col("end_date").between(b_first, day)
    return contracts.filter(started | ended).select("customer_code").distinct().withColumn("contract_changed", F.lit(True))


def evaluate_meters(meters_to_check: DataFrame, zero_window: DataFrame, drops: DataFrame | None = None,
                    changes: DataFrame | None = None) -> DataFrame:
    """meters_to_check: meter_id, transformer_code, customer_code. zero_window: filled readings of the zero lookback.
    drops: consumption_drop(); changes: contract_changes(). RN-07: zero 48 h, or drop > 60% without a contract change."""
    result = meters_to_check.join(zero_streaks(zero_window), "meter_id", "left").fillna({"zero_streak_intervals": 0})
    if drops is not None:
        result = result.join(drops, "meter_id", "left")
    else:
        result = result.withColumn("drop_pct", F.lit(None).cast("double"))
    if changes is not None:
        result = result.join(changes, "customer_code", "left").fillna({"contract_changed": False})
    else:
        result = result.withColumn("contract_changed", F.lit(False))
    zero_48h = F.col("zero_streak_intervals") >= ZERO_STREAK_INTERVALS
    drop_60 = F.coalesce(F.col("drop_pct") > DROP_THRESHOLD, F.lit(False)) & ~F.col("contract_changed")
    reasons = F.concat_ws("; ",
                          F.when(zero_48h, F.lit("zero consumption for 48 h or more")),
                          F.when(drop_60, F.lit("consumption more than 60% below its 8-week average")))
    return (
        result.withColumn("zero_48h", zero_48h)
        .withColumn("drop_60", drop_60)
        .withColumn("is_suspicious", zero_48h | drop_60)
        .withColumn("reason", F.when(zero_48h | drop_60, reasons))
    )


def meters_of_suspicious_transformers(spark: SparkSession, lake: Lake, day: date) -> DataFrame:
    suspicious = spark.read.parquet(suspicious_path(lake)).filter(F.col("date") == F.lit(day)).select("transformer_code")
    return read_meters(spark, lake).join(suspicious, "transformer_code").select(
        "meter_id", "transformer_code", "customer_code")


def run_day(spark: SparkSession, lake: Lake, day: date) -> DataFrame:
    candidates = meters_of_suspicious_transformers(spark, lake, day)
    ids = candidates.select("meter_id")
    (_, _), (b_first, _) = periods(day)
    filled = spark.read.parquet(filled_path(lake)).filter(F.col("date").between(b_first, day)).join(ids, "meter_id")
    zero_window = filled.filter(F.col("date") >= F.lit(day - timedelta(days=ZERO_LOOKBACK_DAYS)))
    changes = contract_changes(read_contracts(spark, lake), day)
    return evaluate_meters(candidates, zero_window, consumption_drop(filled, day), changes).withColumn("date", F.lit(day))


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
