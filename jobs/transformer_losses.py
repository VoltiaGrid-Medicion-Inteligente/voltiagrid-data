"""Transformer losses (US-37, RN-06): read F5 and align its 15-minute intervals to 30 minutes (T-37.1)."""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.lake import Lake
from jobs.readings import parse_utc
from schemas.f5 import F5_RESPONSE_SCHEMA

HALF_HOUR_SECONDS = 30 * 60
QUARTERS_PER_HALF_HOUR = 2


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
