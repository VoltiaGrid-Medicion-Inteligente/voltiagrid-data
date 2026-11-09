"""Load and weather fact of the analytics layer (US-53, KAN-313): one row per transformer and hour.

    python -m jobs.fact_load_weather --date 2013-01-03 --lake ../voltiagrid-api/lake
    python -m jobs.fact_load_weather --run f3-2026-11-04 --lake ../voltiagrid-api/lake

Grain: one transformer in one hour (dimensional model, fact_load_weather_hourly). Business question 5: how does
temperature relate to peak load? Built from F5 (energy delivered to each transformer every 15 minutes), F4 (capacity
of the transformer and its circuit) and F2 (hourly weather).

Measures:
- delivered_kwh: the four quarters of the hour added up (an hour with a missing quarter is dropped, as RN-06 drops an
  incomplete half hour);
- peak_kw: the highest quarter of the hour as average power (kWh of 15 minutes x 4);
- load_pct_capacity: peak_kw / capacity of the transformer (kVA, power factor 1, the same convention as circuit_load);
- temperature_c: F2 temperature of that hour (null when F2 has no reading for it).
Keys: date_key, hour (0-23), transformer_code and circuit_code (F4).

Output: curated/dataset=fact_load_weather_hourly/date=YYYY-MM-DD/
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.analytics import check_no_orphans, date_key
from jobs.dr_events import read_weather
from jobs.estimate_gaps import as_dates
from jobs.inventory import read_meters
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from jobs.transformer_losses import f5_paths, read_f5

QUARTERS_PER_HOUR = 4
FACT_COLUMNS = ["date_key", "hour", "transformer_code", "circuit_code", "delivered_kwh", "peak_kw",
                "load_pct_capacity", "temperature_c", "date"]


def hourly_load(f5: DataFrame) -> DataFrame:
    """(transformer_code, hour_start, delivered_kwh, peak_kw) from the 15-minute F5 rows; complete hours only."""
    return (
        f5.withColumn("hour_start", F.date_trunc("hour", F.col("interval_start")).cast("timestamp_ntz"))
        .groupBy("transformer_code", "hour_start")
        .agg(F.round(F.sum("delivered_kwh"), 4).alias("delivered_kwh"),
             F.round(F.max("delivered_kwh") * QUARTERS_PER_HOUR, 4).alias("peak_kw"),
             F.count("*").alias("_quarters"))
        .filter(F.col("_quarters") == QUARTERS_PER_HOUR)
        .drop("_quarters")
    )


def hourly_temperature(weather: DataFrame) -> DataFrame:
    """(hour_start, temperature_c): one value per hour; if F2 has two rows for an hour, their average."""
    return (weather.filter(F.col("temperature").isNotNull())
            .groupBy(F.date_trunc("hour", F.col("time")).cast("timestamp_ntz").alias("hour_start"))
            .agg(F.round(F.avg("temperature"), 2).alias("temperature_c")))


def load_weather_fact(load: DataFrame, temperature: DataFrame, meters: DataFrame) -> DataFrame:
    transformers = (meters.select("transformer_code", "circuit_code",
                                  F.col("transformer_capacity_kva").cast("double").alias("_capacity_kva"))
                    .dropDuplicates(["transformer_code"]))
    return (
        load.join(transformers, "transformer_code", "left")
        .join(temperature, "hour_start", "left")
        .withColumn("load_pct_capacity", F.round(F.col("peak_kw") / F.col("_capacity_kva"), 4))
        .withColumn("date", F.to_date("hour_start"))
        .withColumn("date_key", date_key("date"))
        .withColumn("hour", F.hour("hour_start"))
        .select(FACT_COLUMNS)
    )


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    meters = read_meters(spark, lake)
    load = hourly_load(read_f5(spark, f5_paths(lake, days)))
    fact = (load_weather_fact(load, hourly_temperature(read_weather(spark, lake)), meters)
            .filter(F.col("date").isin(days)).localCheckpoint(eager=True))
    check_no_orphans(fact, {"transformer_code": meters}, "fact_load_weather_hourly")
    path = lake.curated("fact_load_weather_hourly")
    fact.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")
    return fact


def main() -> None:
    p = argparse.ArgumentParser(description="Load and weather fact per transformer and hour (US-53)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-fact-load-weather")
    days = dates_from_args(spark, args, lake)
    s = run(spark, lake, days).agg(
        F.count("*").alias("rows"), F.max("load_pct_capacity").alias("peak"),
        F.sum(F.col("temperature_c").isNull().cast("int")).alias("no_temp"),
        F.corr("temperature_c", "load_pct_capacity").alias("corr")).first()
    corr = "n/a" if s.corr is None else f"{s.corr:+.3f}"
    print(f"fact_load_weather_hourly: {s.rows} transformer-hours in {len(days)} dates, highest load "
          f"{(s.peak or 0):.1%}, {s.no_temp or 0} hours without temperature, temperature-load correlation {corr}; "
          "no orphan keys")
    spark.stop()


if __name__ == "__main__":
    main()
