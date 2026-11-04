"""Control counts of a cleaning run: nothing lost, nothing duplicated between raw and clean (US-31, CA-08).

    python -m jobs.control_counts --source f3 --ingest-date 2026-11-02 --lake ../voltiagrid-api/lake

Every reading that reached raw must be in clean exactly once per meter and interval:
- lost: a meter-interval that has a reading in raw (clock within ±5 minutes) and no row in clean;
- duplicated: more than one clean row for the same meter and interval.
Both must be 0. Readings with an invalid clock are counted apart: they stay in clean, flagged, without an interval.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.clean_readings import clean_path, normalize_clock
from jobs.lake import add_lake_arguments, lake_from_args
from jobs.readings import read_f3
from jobs.spark import get_spark


def control_counts(raw: DataFrame, clean_rows: DataFrame) -> dict:
    """raw: readings as read from raw (READING_COLUMNS); clean_rows: clean rows of the dates those readings fall in."""
    # raw is many small JSON files: read them once and keep them for the five counts below
    normalized = normalize_clock(raw).localCheckpoint(eager=True)
    keys = normalized.filter("clock_ok").select("meter_id", "interval_start").distinct()
    kept = clean_rows.filter(F.col("interval_start").isNotNull())
    kept_keys = kept.select("meter_id", "interval_start").distinct()
    row = {
        "raw_readings": normalized.count(),
        "raw_meter_intervals": keys.count(),
        "raw_invalid_clock": normalized.filter(~F.col("clock_ok")).count(),
        "clean_rows": kept.count(),
    }
    row["lost"] = keys.join(kept_keys, ["meter_id", "interval_start"], "left_anti").count()
    row["duplicated"] = row["clean_rows"] - kept_keys.count()
    return row


def main() -> None:
    p = argparse.ArgumentParser(description="Control counts between raw and clean (CA-08)")
    add_lake_arguments(p)
    p.add_argument("--source", choices=["f3"], default="f3")
    p.add_argument("--ingest-date", required=True)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-control-counts")
    raw = read_f3(spark, [f"{lake.raw('f3', 'readings')}/ingest_date={args.ingest_date}"]).localCheckpoint(eager=True)
    dates = [r.d for r in normalize_clock(raw).select(F.to_date("interval_start").alias("d")).distinct().collect() if r.d]
    clean_rows = spark.read.parquet(clean_path(lake)).filter(F.col("date").isin(dates))
    counts = control_counts(raw, clean_rows)
    for name, value in counts.items():
        print(f"{name:>20}: {value:,}")
    print("OK: nothing lost, nothing duplicated" if counts["lost"] == counts["duplicated"] == 0 else "CHECK FAILED")
    spark.stop()
    if counts["lost"] or counts["duplicated"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
