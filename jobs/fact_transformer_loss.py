"""Loss fact of the analytics layer (US-51): one row per transformer and day (T-51.1), valued in money (T-51.2).

    python -m jobs.fact_transformer_loss --date 2013-01-15 --lake ../voltiagrid-api/lake
    python -m jobs.fact_transformer_loss --run f3-2026-11-02 --lake ../voltiagrid-api/lake

Grain: one transformer on one day (dimensional model, fact_transformer_loss_daily). Runs after
jobs.transformer_losses and reads what it wrote: the daily losses (RN-06) and the suspicious transformers.
Measures: delivered_kwh, metered_kwh, estimated_kwh, loss_kwh, loss_pct, consecutive_days_over_12 (days in a row up
to this one with a loss above 12%), is_suspicious (0/1) and suspicious_rank. Keys: date_key, transformer_code and
circuit_code (F4).

loss_cost (T-51.2): the loss of each half hour (F5 delivered - meters, the same inputs as RN-06) valued at the F7
price of its band on that date, added up per day. A kWh lost at 19:00 costs the peak price, one lost at 03:00 the
off-peak price, so loss_cost is not loss_kwh times one average price.

Output: curated/dataset=fact_transformer_loss_daily/date=YYYY-MM-DD/
"""

import argparse
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.analytics import check_no_orphans, date_key
from jobs.estimate_gaps import as_dates, filled_path
from jobs.inventory import read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from jobs.tariff_bands import DEFAULT_TARIFFS, assign_bands, read_tariffs
from jobs.transformer_losses import (STREAK_LOOKBACK_DAYS, f5_paths, losses_path, metered_by_transformer, read_f5,
                                     suspicious_path, to_half_hour)
from pipeline.losses import LOSS_THRESHOLD

SUSPICIOUS_SCHEMA = "transformer_code string, rank int, date date"
FACT_COLUMNS = ["date_key", "transformer_code", "circuit_code", "delivered_kwh", "metered_kwh", "estimated_kwh",
                "loss_kwh", "loss_pct", "loss_cost", "consecutive_days_over_12", "is_suspicious", "suspicious_rank",
                "date"]


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


def loss_cost_by_day(delivered: DataFrame, metered: DataFrame, tariffs: DataFrame) -> DataFrame:
    """T-51.2: (transformer_code, date, loss_cost): each half hour's loss at the F7 price of its band, per day.
    Same half hours as RN-06: only the ones present in both F5 and the meters."""
    per_half_hour = (delivered.join(metered, ["transformer_code", "interval_start"])
                     .withColumn("_loss", F.col("delivered_kwh") - F.col("metered_kwh")))
    priced = assign_bands(per_half_hour, tariffs)
    return (priced.withColumn("date", F.to_date("interval_start"))
            .groupBy("transformer_code", "date")
            .agg(F.round(F.sum(F.col("_loss") * F.col("price_per_kwh")), 2).cast("decimal(12,2)").alias("loss_cost")))


def loss_fact(losses: DataFrame, history: DataFrame, suspicious: DataFrame, meters: DataFrame,
              costs: DataFrame) -> DataFrame:
    circuits = meters.select("transformer_code", "circuit_code").distinct()
    return (
        losses.join(consecutive_days_over(history), ["transformer_code", "date"], "left")
        .join(costs, ["transformer_code", "date"], "left")
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
    delivered = to_half_hour(read_f5(spark, f5_paths(lake, days)))
    metered = metered_by_transformer(spark.read.parquet(filled_path(lake)).filter(F.col("date").isin(days)), meters)
    costs = loss_cost_by_day(delivered, metered, read_tariffs(spark, DEFAULT_TARIFFS))
    fact = loss_fact(losses, history, suspicious, meters, costs).localCheckpoint(eager=True)
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
                                   F.sum("loss_cost").alias("cost"), F.sum("is_suspicious").alias("suspicious")).first()
    print(f"fact_transformer_loss_daily: {s.rows} transformer-days in {len(days)} dates, "
          f"{s.loss or 0:,.1f} kWh lost worth {s.cost or 0:,.2f}, {s.suspicious or 0} suspicious; no orphan keys")
    spark.stop()


if __name__ == "__main__":
    main()
