"""Tariff band of each interval (US-35, RN-05): join with the F7 calendar by effective date (T-35.1).

The band is decided by the calendar row valid on the reading date and at the reading time:
    valid_from <= date(interval_start) <= valid_to   and   start_time <= time(interval_start) <= end_time
F7 has fewer than 100 rows, so it is broadcast to every task and the range join costs nothing.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from schemas.f7 import F7_SCHEMA

DEFAULT_TARIFFS = "data/reference/tariffs.csv"


def read_tariffs(spark: SparkSession, path: str = DEFAULT_TARIFFS) -> DataFrame:
    return spark.read.csv(path, header=True, schema=F7_SCHEMA, mode="FAILFAST")


def assign_bands(readings: DataFrame, tariffs: DataFrame) -> DataFrame:
    """Adds band_name, price_per_kwh and tariff_valid_from (which calendar period priced it). Rows with no band keep
    nulls; `missing_bands` counts them so a job can stop instead of billing without a price."""
    t = tariffs.select(
        F.col("valid_from").alias("tariff_valid_from"), F.col("valid_to").alias("_valid_to"),
        F.col("start_time").alias("_start"), F.col("end_time").alias("_end"),
        "band_name", "price_per_kwh",
    )
    reading_date = F.to_date("interval_start")
    reading_time = F.date_format("interval_start", "HH:mm:ss")
    condition = (
        reading_date.between(F.col("tariff_valid_from"), F.col("_valid_to"))
        & reading_time.between(F.col("_start"), F.col("_end"))
    )
    return readings.join(F.broadcast(t), condition, "left").drop("_valid_to", "_start", "_end")


def missing_bands(banded: DataFrame) -> int:
    return banded.filter(F.col("band_name").isNull() & F.col("interval_start").isNotNull()).count()
