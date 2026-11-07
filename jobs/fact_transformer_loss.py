"""Loss fact of the analytics layer (US-51, T-51.1): one row per transformer and day.

    python -m jobs.fact_transformer_loss --date 2013-01-15 --lake ../voltiagrid-api/lake
    python -m jobs.fact_transformer_loss --run f3-2026-11-02 --lake ../voltiagrid-api/lake

Grain: one transformer on one day (dimensional model, fact_transformer_loss_daily). Runs after
jobs.transformer_losses and reads what it wrote: the daily losses (RN-06) and the suspicious transformers.
Measures: delivered_kwh, metered_kwh, estimated_kwh, loss_kwh, loss_pct, consecutive_days_over_12 (days in a row up
to this one with a loss above 12%), is_suspicious (0/1) and suspicious_rank. Keys: date_key, transformer_code and
circuit_code (F4).

Output: curated/dataset=fact_transformer_loss_daily/date=YYYY-MM-DD/
"""

import argparse
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.analytics import check_no_orphans, date_key
from jobs.estimate_gaps import as_dates
from jobs.inventory import read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from jobs.transformer_losses import STREAK_LOOKBACK_DAYS, losses_path, suspicious_path
from pipeline.losses import LOSS_THRESHOLD

SUSPICIOUS_SCHEMA = "transformer_code string, rank int, date date"
FACT_COLUMNS = ["date_key", "transformer_code", "circuit_code", "delivered_kwh", "metered_kwh", "estimated_kwh",
                "loss_kwh", "loss_pct", "consecutive_days_over_12", "is_suspicious", "suspicious_rank", "date"]


def consecutive_days_over(history: DataFrame) -> DataFrame:
    """(transformer_code, date, consecutive_days_over_12): 0 on a day at or below 12%; a missing day breaks the count.

    Same gaps-and-islands idea as RN-06 (jobs.transformer_losses): among the days above the threshold,
    date - row_number() is constant inside a run of consecutive days."""
    over = history.filter(F.col("loss_pct") > LOSS_THRESHOLD).select("transformer_code", "date")
    by_transformer = Window.partitionBy("transformer_code").orderBy("date")
    run_id = F.date_sub(F.col("date"), F.row_number().over(by_transformer))
    along = Window.partitionBy("transformer_code", "_run").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    return (over.withColumn("_run", run_id)
            .withColumn("consecutive_days_over_12", F.count("*").over(along).cast("int"))
            .drop("_run"))


def loss_fact(losses: DataFrame, history: DataFrame, suspicious: DataFrame, meters: DataFrame) -> DataFrame:
    circuits = meters.select("transformer_code", "circuit_code").distinct()
    return (
        losses.join(consecutive_days_over(history), ["transformer_code", "date"], "left")
        .join(suspicious.select("transformer_code", "date", F.col("rank").alias("suspicious_rank")),
              ["transformer_code", "date"], "left")
        .join(circuits, "transformer_code", "left")
        .withColumn("date_key", date_key("date"))
        .withColumn("consecutive_days_over_12", F.coalesce("consecutive_days_over_12", F.lit(0)))
        .withColumn("is_suspicious", F.col("suspicious_rank").isNotNull().cast("int"))
        .select(FACT_COLUMNS)
    )


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    history = spark.read.parquet(losses_path(lake)).filter(
        F.col("date").between(days[0] - timedelta(days=STREAK_LOOKBACK_DAYS), days[-1]))
    losses = history.filter(F.col("date").isin(days))
    suspicious = spark.read.schema(SUSPICIOUS_SCHEMA).parquet(suspicious_path(lake)).filter(F.col("date").isin(days))
    meters = read_meters(spark, lake)
    fact = loss_fact(losses, history, suspicious, meters).localCheckpoint(eager=True)
    check_no_orphans(fact, {"transformer_code": meters}, "fact_transformer_loss_daily")
    path = lake.curated("fact_transformer_loss_daily")
    fact.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")
    return fact


def main() -> None:
    p = argparse.ArgumentParser(description="Loss fact per transformer and day (US-51)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-fact-transformer-loss")
    days = dates_from_args(spark, args, lake)
    s = run(spark, lake, days).agg(F.count("*").alias("rows"), F.sum("loss_kwh").alias("loss"),
                                   F.sum("is_suspicious").alias("suspicious")).first()
    print(f"fact_transformer_loss_daily: {s.rows} transformer-days in {len(days)} dates, "
          f"{s.loss or 0:,.1f} kWh lost, {s.suspicious or 0} suspicious; no orphan keys")
    spark.stop()


if __name__ == "__main__":
    main()
