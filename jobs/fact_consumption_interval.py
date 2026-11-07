"""Consumption fact of the analytics layer (US-48, T-48.1): one row per meter and 30-minute interval.

    python -m jobs.fact_consumption_interval --date 2013-01-15 --lake ../voltiagrid-api/lake
    python -m jobs.fact_consumption_interval --run f3-2026-11-02 --lake ../voltiagrid-api/lake

Grain: one meter in one 30-minute interval (dimensional model, fact_consumption). Built from the filled readings
(real + estimated, US-34), F4 (meter -> customer, transformer) and F7 (band and price valid on the date, RN-05).
Measures: energy_kwh, energy_cost (kWh x band price), is_estimated (0/1) and estimation_method.
Keys (jobs/analytics.py): date_key, time_key, meter_id, customer_code, transformer_code, band_name.

The monthly table curated/dataset=fact_consumption (US-35) stays as it is: it is the billing aggregate. This one is
the detail Power BI reads for the consumption questions.

Output: curated/dataset=fact_consumption_interval/date=YYYY-MM-DD/
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.analytics import check_no_orphans, date_key, time_key
from jobs.estimate_gaps import filled_path
from jobs.inventory import read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from jobs.tariff_bands import DEFAULT_TARIFFS, assign_bands, read_tariffs

FACT_COLUMNS = ["date_key", "time_key", "meter_id", "customer_code", "transformer_code", "band_name",
                "tariff_valid_from", "interval_start", "energy_kwh", "energy_cost", "is_estimated",
                "estimation_method", "date"]


def consumption_interval(filled: DataFrame, meters: DataFrame, tariffs: DataFrame) -> DataFrame:
    banded = assign_bands(filled.select("meter_id", "interval_start", "energy_kwh", "is_estimated",
                                        "estimation_method", "date"), tariffs)
    return (
        banded.join(meters.select("meter_id", "customer_code", "transformer_code"), "meter_id", "left")
        .withColumn("date_key", date_key("date"))
        .withColumn("time_key", time_key("interval_start"))
        .withColumn("energy_cost", F.round(F.col("energy_kwh") * F.col("price_per_kwh"), 4).cast("decimal(12,4)"))
        .withColumn("is_estimated", F.col("is_estimated").cast("int"))
        .withColumn("estimation_method", F.coalesce("estimation_method", F.lit("none")))
        .select(FACT_COLUMNS)
    )


def check(fact: DataFrame, meters: DataFrame, tariffs: DataFrame) -> None:
    """Every meter is in F4 (so are its customer and transformer) and every interval got a band of F7."""
    check_no_orphans(fact, {"meter_id": meters, "customer_code": meters, "transformer_code": meters,
                            "band_name": tariffs}, "fact_consumption_interval")


def run(spark: SparkSession, lake: Lake, dates: list[str], tariffs_path: str = DEFAULT_TARIFFS) -> DataFrame:
    filled = spark.read.parquet(filled_path(lake)).filter(F.col("date").isin(dates))
    meters, tariffs = read_meters(spark, lake), read_tariffs(spark, tariffs_path)
    fact = consumption_interval(filled, meters, tariffs).localCheckpoint(eager=True)
    check(fact, meters, tariffs)
    path = lake.curated("fact_consumption_interval")
    fact.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in dates:
        write_text(spark, f"{path}/date={d}/_SUCCESS", "")
    return fact


def main() -> None:
    p = argparse.ArgumentParser(description="Consumption fact per meter and 30-minute interval (US-48)")
    add_lake_arguments(p)
    add_date_arguments(p)
    p.add_argument("--tariffs", default=DEFAULT_TARIFFS)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-fact-consumption-interval")
    dates = dates_from_args(spark, args, lake)
    s = run(spark, lake, dates, args.tariffs).agg(
        F.count("*").alias("rows"), F.countDistinct("meter_id").alias("meters"), F.sum("energy_kwh").alias("kwh"),
        F.sum("energy_cost").alias("cost"), F.sum("is_estimated").alias("estimated")).first()
    print(f"fact_consumption_interval: {s.rows:,} rows ({s.meters} meters x intervals) in {len(dates)} dates, "
          f"{s.kwh or 0:,.1f} kWh, cost {s.cost or 0:,.2f}, {s.estimated or 0:,} estimated; no orphan keys")
    spark.stop()


if __name__ == "__main__":
    main()
