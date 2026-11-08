"""Dimensions of the analytics layer (US-52, T-52.1): date, month, time of day, tariff band, customer, meter, transformer.

    python -m jobs.dimensions --lake ../voltiagrid-api/lake --from 2012-12-01 --to 2015-12-31
    python -m jobs.dimensions --lake ../voltiagrid-api/lake --ingest-date 2026-11-01

Built from the dimensional model (voltiagrid-docs, docs/data/dimensional-model.md, section Dimensions):
- generated: dim_date (one row per day), dim_month (roll-up of dim_date), dim_time_of_day (48 half hours);
- F7: dim_tariff_band, one row per band version (type 2 by effective date, as the calendar already versions it);
- F4: dim_customer (one row per contract version, type 2 on contract change), dim_meter, dim_transformer;
- US-43 (dimensional model, section Dimensions): dim_dr_event, one row per demand-response event, read from curated dr_events.

Keys are the ones of jobs/analytics.py: date_key = yyyymmdd, time_key = slot 0..47, and the F4 codes for the rest, so
the facts of US-45, US-48 and US-51 join without a lookup. Output: curated/dataset=dim_<name>/ (one file, no partition).

Not in F4 yet: ACORN group (dim_customer.acorn_group). The job writes the columns that exist and leaves ACORN out
rather than inventing it; it is an open point in the dimensional model.
"""

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.analytics import date_key
from jobs.dr_events import read_events
from jobs.inventory import read_contracts, read_meters
from jobs.lake import Lake, add_lake_arguments, lake_from_args
from jobs.spark import get_spark
from jobs.tariff_bands import DEFAULT_TARIFFS, read_tariffs

OPEN_END = date(9999, 12, 31)


def date_dim(spark: SparkSession, start: str, end: str) -> DataFrame:
    """One row per day from start to end, both included."""
    days = spark.range(1).select(F.explode(F.sequence(F.to_date(F.lit(start)), F.to_date(F.lit(end)),
                                                       F.expr("interval 1 day"))).alias("date"))
    return (days.select(date_key("date").alias("date_key"), "date",
                        F.dayofweek("date").alias("_dow"))  # Spark: 1 = Sunday
            .select("date_key", "date",
                    ((F.col("_dow") + 5) % 7 + 1).alias("day_of_week"),  # ISO: 1 = Monday ... 7 = Sunday
                    (F.col("_dow").between(2, 6)).alias("is_business_day"),  # holidays not modelled yet
                    F.date_format("date", "yyyy-MM").alias("year_month"),
                    F.month("date").alias("month"),
                    F.quarter("date").alias("quarter"),
                    F.year("date").alias("year")))


def month_dim(dates: DataFrame) -> DataFrame:
    """Roll-up of dim_date: one row per calendar month."""
    return (dates.groupBy("year_month", "year", "quarter", "month")
            .agg(F.min("date_key").alias("first_date_key"))
            .select(F.col("year_month"), F.date_format(F.to_date(F.concat("year_month", F.lit("-01"))), "MMMM")
                    .alias("month_name"), "quarter", "year", "first_date_key")
            .orderBy("year_month"))


def time_of_day_dim(spark: SparkSession) -> DataFrame:
    """The 48 half hours of a day, slot 0 (00:00) to 47 (23:30): the values time_key produces."""
    slots = spark.range(48).select(F.col("id").cast("int").alias("time_key"))
    return slots.select("time_key",
                        F.format_string("%02d:%02d", F.floor(F.col("time_key") / 2).cast("int"),
                                        (F.col("time_key") % 2) * 30).alias("hh_mm"),
                        F.floor(F.col("time_key") / 2).cast("int").alias("hour"))


def tariff_band_dim(tariffs: DataFrame) -> DataFrame:
    """One row per F7 band version: the same rows, typed for the join (RN-05)."""
    return tariffs.select("band_name", "price_per_kwh", "start_time", "end_time", "valid_from", "valid_to")


def customer_dim(contracts: DataFrame) -> DataFrame:
    """One row per contract version (type 2): valid_from/valid_to from the F4 start and end dates; the open one is
    is_current = 1 and its valid_to is 9999-12-31."""
    return contracts.select(
        "customer_code", "contract_code", "tariff_type",
        F.col("start_date").alias("valid_from"),
        F.coalesce(F.col("end_date"), F.lit(OPEN_END)).alias("valid_to"),
        F.col("end_date").isNull().cast("int").alias("is_current"))


def meter_dim(meters: DataFrame) -> DataFrame:
    """One row per meter: lclid is the meter_id of F4 (the code used everywhere in the lake)."""
    return meters.select(F.col("meter_id").alias("lclid"), "customer_code", "transformer_code").dropDuplicates(["lclid"])


def transformer_dim(meters: DataFrame) -> DataFrame:
    """One row per transformer, with its circuit (F4 meters carry both)."""
    return (meters.select("transformer_code", F.col("transformer_capacity_kva").alias("capacity_kva"),
                          "circuit_code", F.col("circuit_capacity_kva").alias("circuit_capacity_kva"))
            .dropDuplicates(["transformer_code"]))


def dr_event_dim(events: DataFrame) -> DataFrame:
    """One row per demand-response event (RN-08): its circuit, window, duration and the two values that declared it
    (forecast temperature and projected load). Read from curated dr_events, so the facts of US-43 join on event_id."""
    return events.select("event_id", "circuit_code", "start_ts", "end_ts", "duration_h", "forecast_temp_c",
                         "projected_load_pct")


def write_dim(df: DataFrame, lake: Lake, name: str) -> int:
    """Curated, one file, overwritten on every run: a rerun gives the same table (CT-08)."""
    df.coalesce(1).write.mode("overwrite").parquet(lake.curated(f"dim_{name}"))
    return df.count()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_lake_arguments(p)
    p.add_argument("--from", dest="start", default="2012-12-01", help="first day of dim_date (YYYY-MM-DD)")
    p.add_argument("--to", dest="end", default="2015-12-31", help="last day of dim_date (YYYY-MM-DD)")
    p.add_argument("--tariffs", default=DEFAULT_TARIFFS, help="F7 tariff calendar (CSV)")
    p.add_argument("--ingest-date", default=None, help="raw partition of F4 (default: the latest one)")
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-dimensions", ui=False)
    meters = read_meters(spark, lake, args.ingest_date)
    contracts = read_contracts(spark, lake, args.ingest_date)
    dates = date_dim(spark, args.start, args.end)
    tables = {
        "date": dates,
        "month": month_dim(dates),
        "time_of_day": time_of_day_dim(spark),
        "tariff_band": tariff_band_dim(read_tariffs(spark, args.tariffs)),
        "customer": customer_dim(contracts),
        "meter": meter_dim(meters),
        "transformer": transformer_dim(meters),
        "dr_event": dr_event_dim(read_events(spark, lake)),
    }
    for name, df in tables.items():
        print(f"dimensions: dim_{name}: {write_dim(df, lake, name)} rows")
    spark.stop()


if __name__ == "__main__":
    main()
