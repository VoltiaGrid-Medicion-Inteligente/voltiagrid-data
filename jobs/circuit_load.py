"""Load of each circuit as a share of its capacity, every 30 minutes (US-33, T-33.1).

    python -m jobs.circuit_load --date 2013-01-01 --lake ../voltiagrid-api/lake
    python -m jobs.circuit_load --run f3-2026-11-02 --lake ../voltiagrid-api/lake

For each circuit and interval: kWh of its meters (valid clean readings) -> average kW = kWh x 2 (30 minutes) ->
load_pct = kW / capacity (kVA, from F4, power factor 1). Above 85% the interval is flagged (US-33; the same
threshold starts a demand-response event in RN-08).

Output: curated/dataset=circuit_load/date=YYYY-MM-DD/
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.clean_readings import clean_path
from jobs.inventory import circuit_capacity, read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark

OVERLOAD_THRESHOLD = 0.85
INTERVALS_PER_HOUR = 2


def circuit_load(readings: DataFrame, meters: DataFrame) -> DataFrame:
    """readings: clean rows (meter_id, interval_start, energy_kwh, is_valid); meters: F4 meters."""
    valid = readings.filter(F.col("is_valid")).select("meter_id", "interval_start", "energy_kwh")
    per_circuit = (
        valid.join(F.broadcast(meters.select("meter_id", "circuit_code")), "meter_id")  # F4 is small (US-42)
        .groupBy("circuit_code", "interval_start")
        .agg(F.sum("energy_kwh").alias("energy_kwh"), F.countDistinct("meter_id").alias("meters_reporting"))
    )
    capacity = circuit_capacity(meters).select("circuit_code", F.col("capacity_kva").cast("double"), "meters")
    return (
        per_circuit.join(capacity, "circuit_code")
        .withColumn("load_kw", F.col("energy_kwh") * INTERVALS_PER_HOUR)
        .withColumn("load_pct", F.round(F.col("load_kw") / F.col("capacity_kva"), 4))
        .withColumn("is_overloaded", F.col("load_pct") > OVERLOAD_THRESHOLD)
        .withColumn("date", F.to_date("interval_start"))
        .select("circuit_code", "interval_start", "energy_kwh", "load_kw", "capacity_kva", "load_pct",
                "is_overloaded", "meters_reporting", F.col("meters").alias("meters_total"), "date")
    )


def run(spark: SparkSession, lake: Lake, dates: list[str]) -> DataFrame:
    readings = spark.read.parquet(clean_path(lake)).filter(F.col("date").isin(dates))
    result = circuit_load(readings, read_meters(spark, lake))
    path = lake.curated("circuit_load")
    result.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in dates:
        write_text(spark, f"{path}/date={d}/_SUCCESS", "")
    return spark.read.parquet(path).filter(F.col("date").isin(dates))


def main() -> None:
    p = argparse.ArgumentParser(description="Circuit load as a share of capacity (US-33)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-circuit-load")
    dates = dates_from_args(spark, args, lake)
    result = run(spark, lake, dates)
    summary = result.agg(F.count("*").alias("rows"), F.max("load_pct").alias("peak"),
                         F.sum(F.col("is_overloaded").cast("int")).alias("overloaded")).first()
    print(f"circuit_load: {summary.rows:,} circuit-intervals in {len(dates)} dates, "
          f"peak {summary.peak or 0:.1%}, {summary.overloaded or 0} above {OVERLOAD_THRESHOLD:.0%}")
    spark.stop()


if __name__ == "__main__":
    main()
