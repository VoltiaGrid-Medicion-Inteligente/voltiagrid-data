"""Monthly consumption per customer and tariff band (US-35, T-35.2) -> curated fact_consumption.

    python -m jobs.fact_consumption --month 2013-01 --lake ../voltiagrid-api/lake
    python -m jobs.fact_consumption --run f3-2026-11-02 --lake ../voltiagrid-api/lake

Input: the filled readings (real + estimated, US-34), F4 meters (meter -> customer) and F7 (data/reference/tariffs.csv).
One row per customer, month, band and calendar period: if the tariff changes in the middle of the month, the same
band appears twice, each with its own price and kWh (the month is split, US-35).

Output: curated/dataset=fact_consumption/month=YYYY-MM/
"""

import argparse
import calendar
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.estimate_gaps import filled_path
from jobs.inventory import read_meters
from jobs.lake import Lake, add_lake_arguments, add_month_arguments, lake_from_args, months_from_args, write_text
from jobs.spark import get_spark
from jobs.tariff_bands import DEFAULT_TARIFFS, assign_bands, missing_bands, read_tariffs

FACT_COLUMNS = ["customer_code", "band_name", "tariff_valid_from", "price_per_kwh", "energy_kwh", "estimated_kwh",
                "intervals", "estimated_intervals", "amount", "month"]


def monthly_consumption(filled: DataFrame, meters: DataFrame, tariffs: DataFrame) -> DataFrame:
    banded = assign_bands(
        filled.select("meter_id", "interval_start", "energy_kwh", "is_estimated"), tariffs
    ).join(F.broadcast(meters.select("meter_id", "customer_code")), "meter_id")  # F4 is small (US-42)
    estimated = F.col("is_estimated")
    return (
        banded.withColumn("month", F.date_format("interval_start", "yyyy-MM"))
        .groupBy("customer_code", "month", "band_name", "tariff_valid_from", "price_per_kwh")
        .agg(
            F.round(F.sum("energy_kwh"), 4).alias("energy_kwh"),
            F.round(F.sum(F.when(estimated, F.col("energy_kwh")).otherwise(0.0)), 4).alias("estimated_kwh"),
            F.count("*").alias("intervals"),
            F.sum(estimated.cast("int")).alias("estimated_intervals"),
        )
        .withColumn("amount", F.round(F.col("energy_kwh") * F.col("price_per_kwh"), 2).cast("decimal(12,2)"))
        .select(FACT_COLUMNS)
    )


def month_range(month: str) -> tuple[date, date]:
    year, mon = map(int, month.split("-"))
    return date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])


def run(spark: SparkSession, lake: Lake, months: list[str], tariffs_path: str = DEFAULT_TARIFFS) -> DataFrame:
    condition = None
    for m in months:
        first, last = month_range(m)
        part = F.col("date").between(first, last)
        condition = part if condition is None else condition | part
    filled = spark.read.parquet(filled_path(lake)).filter(condition)
    tariffs = read_tariffs(spark, tariffs_path)
    missing = missing_bands(assign_bands(filled.select("interval_start"), tariffs))
    if missing:
        raise SystemExit(f"{missing} intervals have no tariff band in {tariffs_path}: fix F7 before billing")
    result = monthly_consumption(filled, read_meters(spark, lake), tariffs)
    path = lake.curated("fact_consumption")
    result.repartition("month").write.mode("overwrite").partitionBy("month").parquet(path)
    for m in months:
        write_text(spark, f"{path}/month={m}/_SUCCESS", "")
    return spark.read.parquet(path).filter(F.col("month").isin(months))


def main() -> None:
    p = argparse.ArgumentParser(description="Monthly consumption per customer and band (US-35)")
    add_lake_arguments(p)
    add_month_arguments(p)
    p.add_argument("--tariffs", default=DEFAULT_TARIFFS)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-fact-consumption")
    months = months_from_args(spark, args, lake)
    result = run(spark, lake, months, args.tariffs)
    s = result.agg(F.countDistinct("customer_code").alias("customers"), F.count("*").alias("rows"),
                   F.sum("energy_kwh").alias("kwh"), F.sum("amount").alias("amount")).first()
    print(f"fact_consumption: {s.rows:,} rows, {s.customers:,} customers, {s.kwh or 0:,.1f} kWh, "
          f"amount {s.amount or 0:,.2f} in {', '.join(months)}")
    spark.stop()


if __name__ == "__main__":
    main()
