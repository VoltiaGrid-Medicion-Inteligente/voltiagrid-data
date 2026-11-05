"""Transformer losses (US-37, RN-06): F5 aligned to 30 minutes (T-37.1), daily loss per transformer (T-37.2) and
3 consecutive days above 12% (T-37.3).

    python -m jobs.fetch_f5 --date 2013-01-05 --lake ../voltiagrid-api/lake        # first, F5 to raw
    python -m jobs.transformer_losses --date 2013-01-05 --lake ../voltiagrid-api/lake

loss_kwh = delivered (F5) - sum of the transformer's meters in the same intervals (filled readings, US-34)
loss_pct = loss_kwh / delivered
suspicious = loss_pct > 12% on 3 or more consecutive days
Output: curated/dataset=transformer_losses/date=YYYY-MM-DD/       every transformer and day
        curated/dataset=suspicious_transformers/date=YYYY-MM-DD/  suspicious ones, ranked by energy lost
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.estimate_gaps import as_dates, filled_path
from jobs.inventory import read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.readings import parse_utc
from jobs.spark import get_spark
from pipeline.losses import CONSECUTIVE_DAYS, LOSS_THRESHOLD  # RN-06, shared with the rule
from schemas.f5 import F5_RESPONSE_SCHEMA

HALF_HOUR_SECONDS = 30 * 60
QUARTERS_PER_HALF_HOUR = 2
STREAK_LOOKBACK_DAYS = 31


def f5_paths(lake: Lake, days: list[date]) -> list[str]:
    """Every raw object of those days, whatever day it was fetched on."""
    base = lake.raw("f5", "transformer_energy")
    return [f"{base}/ingest_date=*/*_{d:%Y%m%d}T0000Z_*.json" for d in days]


def read_f5(spark: SparkSession, paths: list[str]) -> DataFrame:
    """One row per transformer and 15-minute interval. A day fetched twice counts once."""
    responses = spark.read.schema(F5_RESPONSE_SCHEMA).option("multiLine", True).json(paths)
    return (
        responses.select("transformer_code", F.explode("readings").alias("r"))
        .select("transformer_code", parse_utc(F.col("r.interval_start")).alias("interval_start"),
                F.col("r.delivered_kwh").alias("delivered_kwh"))
        .dropDuplicates(["transformer_code", "interval_start"])
    )


def to_half_hour(f5: DataFrame) -> DataFrame:
    """Add the two quarters of each half hour (:00 + :15, :30 + :45), the interval of the meters (F3, 30 minutes).

    A half hour with only one quarter is incomplete: comparing it with 30 minutes of meters would invent a loss, so it
    is dropped. Columns: transformer_code, interval_start (30 min), delivered_kwh.
    """
    seconds = F.unix_timestamp(F.col("interval_start").cast("timestamp"))
    half_hour = F.timestamp_seconds(seconds - F.pmod(seconds, F.lit(HALF_HOUR_SECONDS))).cast("timestamp_ntz")
    return (
        f5.withColumn("interval_start", half_hour)
        .groupBy("transformer_code", "interval_start")
        .agg(F.round(F.sum("delivered_kwh"), 4).alias("delivered_kwh"), F.count("*").alias("_quarters"))
        .filter(F.col("_quarters") == QUARTERS_PER_HALF_HOUR)
        .drop("_quarters")
    )


def metered_by_transformer(filled: DataFrame, meters: DataFrame) -> DataFrame:
    """Sum of the transformer's meters per half hour, real and estimated readings (filled, US-34).

    Using the filled readings matters: with only real ones, every missing reading would look like stolen energy.
    """
    return (
        filled.join(meters.select("meter_id", "transformer_code"), "meter_id")
        .groupBy("transformer_code", "interval_start")
        .agg(
            F.sum("energy_kwh").alias("metered_kwh"),
            F.sum(F.when(F.col("is_estimated"), F.col("energy_kwh")).otherwise(0.0)).alias("estimated_kwh"),
            F.countDistinct("meter_id").alias("meters"),
        )
    )


def daily_losses(delivered: DataFrame, metered: DataFrame) -> DataFrame:
    """RN-06, first part: loss per transformer and day, over the half hours present in both F5 and the meters."""
    return (
        delivered.join(metered, ["transformer_code", "interval_start"])
        .withColumn("date", F.to_date("interval_start"))
        .groupBy("transformer_code", "date")
        .agg(
            F.round(F.sum("delivered_kwh"), 4).alias("delivered_kwh"),
            F.round(F.sum("metered_kwh"), 4).alias("metered_kwh"),
            F.round(F.sum("estimated_kwh"), 4).alias("estimated_kwh"),
            F.count("*").alias("intervals"),
            F.max("meters").alias("meters"),
        )
        .withColumn("loss_kwh", F.round(F.col("delivered_kwh") - F.col("metered_kwh"), 4))
        .withColumn("loss_pct", F.round(F.col("loss_kwh") / F.col("delivered_kwh"), 4))
        .select("transformer_code", "delivered_kwh", "metered_kwh", "loss_kwh", "loss_pct", "estimated_kwh",
                "intervals", "meters", "date")
    )


def suspicious_transformers(losses: DataFrame) -> DataFrame:
    """RN-06, second part (T-37.3): a transformer is suspicious on a day when its loss was above 12% that day and the
    two days before it (3 or more consecutive days). A day without a loss value breaks the streak.

    Gaps and islands: for the days above the threshold, date - row_number() is the same for consecutive days, so it
    names each streak. streak_days and streak_loss_kwh accumulate along the streak. Suspicious transformers of a day
    are ranked by the energy lost during their streak (1 = most).
    """
    by_transformer = Window.partitionBy("transformer_code").orderBy("date")
    over = losses.filter(F.col("loss_pct") > LOSS_THRESHOLD)
    streaks = over.withColumn("_streak", F.date_sub(F.col("date"), F.row_number().over(by_transformer)))
    along = Window.partitionBy("transformer_code", "_streak").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    evaluated = (
        streaks.withColumn("streak_days", F.count("*").over(along))
        .withColumn("streak_start", F.min("date").over(along))
        .withColumn("streak_loss_kwh", F.round(F.sum("loss_kwh").over(along), 4))
        .filter(F.col("streak_days") >= CONSECUTIVE_DAYS)
    )
    ranking = Window.partitionBy("date").orderBy(F.col("streak_loss_kwh").desc(), "transformer_code")
    return evaluated.withColumn("rank", F.row_number().over(ranking)).select(
        "transformer_code", "rank", "streak_days", "streak_start", "streak_loss_kwh", "loss_pct", "loss_kwh",
        "delivered_kwh", "metered_kwh", "date",
    )


def losses_path(lake: Lake) -> str:
    return lake.curated("transformer_losses")


def suspicious_path(lake: Lake) -> str:
    return lake.curated("suspicious_transformers")


def _write_by_date(spark: SparkSession, df: DataFrame, path: str, days: list[date]) -> None:
    df.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    """Daily losses of the given days, then the streak evaluation with up to 31 days of history before them."""
    days = as_dates(days)
    delivered = to_half_hour(read_f5(spark, f5_paths(lake, days)))
    filled = spark.read.parquet(filled_path(lake)).filter(F.col("date").isin(days))
    losses = daily_losses(delivered, metered_by_transformer(filled, read_meters(spark, lake)))
    _write_by_date(spark, losses, losses_path(lake), days)

    history = spark.read.parquet(losses_path(lake)).filter(
        F.col("date").between(days[0] - timedelta(days=STREAK_LOOKBACK_DAYS), days[-1]))
    suspicious = suspicious_transformers(history).filter(F.col("date").isin(days)).localCheckpoint(eager=True)
    _write_by_date(spark, suspicious, suspicious_path(lake), days)
    return suspicious


def main() -> None:
    p = argparse.ArgumentParser(description="Daily loss per transformer and suspicious transformers (US-37, RN-06)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-transformer-losses")
    days = dates_from_args(spark, args, lake)
    suspicious = run(spark, lake, days)
    losses = spark.read.parquet(losses_path(lake)).filter(F.col("date").isin(days))
    for r in losses.orderBy("date", F.col("loss_pct").desc_nulls_last()).collect():
        if r.metered_kwh is None or r.loss_pct is None:  # e.g. a day whose meters were all 'unavailable'
            print(f"{r.date} {r.transformer_code}: delivered {r.delivered_kwh or 0:,.1f} kWh, no meter data to compare")
            continue
        print(f"{r.date} {r.transformer_code}: delivered {r.delivered_kwh:,.1f} kWh, meters {r.metered_kwh:,.1f}, "
              f"loss {r.loss_pct:.1%} ({r.intervals} half hours)")
    for r in suspicious.orderBy("date", "rank").collect():
        print(f"SUSPICIOUS {r.date} #{r.rank} {r.transformer_code}: {r.streak_days} days above "
              f"{LOSS_THRESHOLD:.0%} since {r.streak_start}, {r.streak_loss_kwh:,.1f} kWh lost")
    spark.stop()


if __name__ == "__main__":
    main()
