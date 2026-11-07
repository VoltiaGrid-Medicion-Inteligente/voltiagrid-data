"""Clean readings: raw -> clean (US-30). Clock (T-30.2), duplicates (T-30.3), quality flags and Parquet (T-30.4).

    python -m jobs.clean_readings --source f3 --ingest-date 2026-11-02 --lake ../voltiagrid-api/lake
    python -m jobs.clean_readings --source f1 --input data/raw --mode dev          # the 4 dev blocks
    python -m jobs.clean_readings --source f1 --ingest-date 2026-10-28 --mode full # F1 history from raw

Output: clean/source=readings/dataset=interval_30min/date=YYYY-MM-DD/ (Parquet, Snappy), one row per meter and
interval plus the readings with an invalid clock, and a run manifest in _runs/ with the dates it touched.
"""

import argparse
import json
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.lake import Lake, add_lake_arguments, lake_from_args, path_exists, write_text
from jobs.readings import READING_COLUMNS, read_f1, read_f3
from jobs.spark import get_spark

HALF_HOUR_SECONDS = 30 * 60
CLOCK_TOLERANCE_SECONDS = 5 * 60
MAX_KWH_PER_INTERVAL = 25.0
NULL_TEXTS = ("", "NULL", "NAN", "NONE")
CLEAN_COLUMNS = ["meter_id", "interval_start", "energy_kwh", "is_valid", "quality_flag",
                 "original_ts", "energy_text", "seq", "source", "date"]


def normalize_clock(df: DataFrame) -> DataFrame:
    """RN-01: move each reading to the nearest :00 or :30 if it is at most 5 minutes away; otherwise invalid clock.

    Adds:
    - interval_start (timestamp_ntz): the normalized interval, null when the clock is invalid.
    - clock_ok (boolean).
    10:33 -> 10:30; 10:27 -> 10:30; 23:57 -> 00:00 of the next day; 10:41 -> invalid (11 minutes from 10:30).
    """
    seconds = F.unix_timestamp(F.col("original_ts").cast("timestamp"))  # session time zone is UTC
    offset = F.pmod(seconds, F.lit(HALF_HOUR_SECONDS))
    nearest = (
        F.when(offset <= CLOCK_TOLERANCE_SECONDS, seconds - offset)
        .when(offset >= HALF_HOUR_SECONDS - CLOCK_TOLERANCE_SECONDS, seconds - offset + HALF_HOUR_SECONDS)
    )
    return (
        df.withColumn("interval_start", F.timestamp_seconds(nearest).cast("timestamp_ntz"))
        .withColumn("clock_ok", F.col("interval_start").isNotNull())
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Two passes, after normalize_clock:

    1. Exact copies (same meter, source, original time, sequence and value) become one row. They come from redeliveries:
       a message stored twice after a consumer crash (ADR-007), or the same F1 block read twice.
    2. RN-02: among the readings of one meter that fall in the same interval, the highest sequence wins, whatever
       its value (a null with sequence 5 beats 0.30 with sequence 3; the quality step flags it later).
       Ties (F1 has no sequence) are broken by the latest original time, then the highest text value, so the
       result does not depend on how Spark split the data.
    Readings with an invalid clock have no interval: they skip pass 2 and stay, flagged, for the quality report.
    """
    exact = df.dropDuplicates(["meter_id", "source", "original_ts", "seq", "energy_text"])
    valid, invalid = exact.filter(F.col("clock_ok")), exact.filter(~F.col("clock_ok"))
    window = Window.partitionBy("meter_id", "interval_start").orderBy(
        F.col("seq").desc(), F.col("original_ts").desc(), F.col("energy_text").desc_nulls_last()
    )
    winners = (
        valid.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    return winners.unionByName(invalid)


def quality_flags(df: DataFrame) -> DataFrame:
    """Flag each reading and keep its energy only when it is usable (US-30, T-30.4). First matching flag wins:

    invalid_clock (RN-01) > null_value (null, "Null", empty) > not_a_number > negative (< 0) > out_of_range (> 25 kWh
    in 30 minutes, physically impossible for a household) > ok.
    Flagged readings stay in clean, with energy_kwh = null and is_valid = false: they are excluded from every
    calculation and their interval is estimated like a gap (US-34). 0 kWh is valid (RN-07 looks for zeros).
    """
    number = F.col("energy_text").try_cast("double")
    flag = (
        F.when(~F.col("clock_ok"), "invalid_clock")
        .when(F.col("energy_text").isNull() | F.upper(F.col("energy_text")).isin(*NULL_TEXTS), "null_value")
        .when(number.isNull(), "not_a_number")
        .when(number < 0, "negative")
        .when(number > MAX_KWH_PER_INTERVAL, "out_of_range")
        .otherwise("ok")
    )
    return (
        df.withColumn("quality_flag", flag)
        .withColumn("is_valid", F.col("quality_flag") == "ok")
        .withColumn("energy_kwh", F.when(F.col("is_valid"), number))
        .withColumn("date", F.to_date(F.coalesce(F.col("interval_start"), F.col("original_ts"))))
    )


def clean(df: DataFrame) -> DataFrame:
    """Raw readings (READING_COLUMNS) -> clean rows (CLEAN_COLUMNS)."""
    return quality_flags(deduplicate(normalize_clock(df))).select(CLEAN_COLUMNS)


def clean_path(lake: Lake) -> str:
    return lake.clean("readings", "interval_30min")


def merge_and_write(spark: SparkSession, new_raw: DataFrame, path: str) -> tuple[DataFrame, list]:
    """Clean the new readings together with what clean already has for the same dates, and overwrite only those dates.

    A resend brings old readings in today's raw partition. Rewriting their date with only the new rows would delete
    what was cleaned before, so the existing rows are read back (they keep the raw columns) and the rules run again
    on the union. Running the same input twice gives the same result (CT-08).
    """
    # Parse the raw files once (US-42, T-42.2): the dates and the cleaning both read them; without this, Spark parses
    # every JSON or CSV file twice.
    new_raw = new_raw.localCheckpoint(eager=True)
    reading_date = F.to_date(F.coalesce(F.col("interval_start"), F.col("original_ts"))).alias("date")
    dates = sorted({r.date for r in normalize_clock(new_raw).select(reading_date).distinct().collect() if r.date})
    combined = new_raw
    if dates and path_exists(spark, path):
        existing = spark.read.parquet(path).filter(F.col("date").isin(dates)).select(READING_COLUMNS)
        combined = new_raw.unionByName(existing)
    result = clean(combined)
    # The output overwrites partitions it is reading from: materialize it first so nothing is read after the delete.
    result = result.localCheckpoint(eager=True)
    (result.repartition("date").write.mode("overwrite").partitionBy("date")
     .option("compression", "snappy").parquet(path))
    for d in dates:
        write_text(spark, f"{path}/date={d}/_SUCCESS", "")
    return result, dates


def f1_paths(base: str, mode: str) -> list[str]:
    if mode == "dev":
        from dev_subset import load_dev_blocks

        return [f"{base}/{block}.csv" for block in load_dev_blocks()]
    return [f"{base}/block_*.csv"]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Clean readings: raw -> clean (US-30)")
    add_lake_arguments(p)
    p.add_argument("--source", choices=["f1", "f3"], required=True)
    p.add_argument("--ingest-date", default=None, help="raw partition to read (YYYY-MM-DD)")
    p.add_argument("--input", default=None, help="read this folder instead of the raw partition (e.g. data/raw)")
    p.add_argument("--mode", choices=["dev", "full"], default="dev", help="F1 only: dev = the 4 dev subset blocks")
    p.add_argument("--driver-memory", default="4g")
    return p


def run_id(args) -> str:
    where = args.ingest_date or "local"
    return f"f3-{where}" if args.source == "f3" else f"f1-{args.mode}-{where}"


def main() -> None:
    args = build_parser().parse_args()
    if not args.ingest_date and not args.input:
        raise SystemExit("give --ingest-date (raw partition) or --input (folder)")
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-clean-readings", driver_memory=args.driver_memory)
    started = time.monotonic()

    if args.source == "f3":
        base = args.input or f"{lake.raw('f3', 'readings')}/ingest_date={args.ingest_date}"
        raw = read_f3(spark, [base])
    else:
        base = args.input or f"{lake.raw('f1', 'halfhourly')}/ingest_date={args.ingest_date}"
        raw = read_f1(spark, f1_paths(base, args.mode))

    path = clean_path(lake)
    result, dates = merge_and_write(spark, raw, path)
    flags = {r.quality_flag: r["count"] for r in result.groupBy("quality_flag").count().collect()}
    manifest = {
        "run": run_id(args),
        "source": args.source,
        "input": base,
        "dates": [d.isoformat() for d in dates],
        "rows": sum(flags.values()),
        "flags": flags,
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(time.monotonic() - started, 1),
    }
    write_text(spark, f"{path}/_runs/{manifest['run']}.json", json.dumps(manifest, indent=2))
    print(f"clean: {manifest['rows']:,} rows in {len(dates)} dates, {manifest['seconds']} s; flags {flags}")
    print(f"run manifest: {path}/_runs/{manifest['run']}.json")
    spark.stop()


if __name__ == "__main__":
    main()
