"""Control counts between the layers of one cleaning run: raw -> clean -> filled -> curated (US-41, T-41.3; CA-09).

    python -m jobs.layer_counts --run f1-full-2026-10-28 --lake ../voltiagrid-api/lake
    python -m jobs.layer_counts --run f3-2026-11-04 --raw s3a://... --clean s3a://... --curated s3a://...

Reads the run manifest (which source, which raw folder, which dates) and checks that nothing was lost on the way:

    raw -> clean      every meter-interval of raw (valid clock) is in clean, once     (jobs.control_counts, CA-08)
    clean -> filled   every valid clean reading is in filled as a real reading
    filled -> curated the 30-minute intervals and kWh of the run's months in fact_consumption equal the filled ones
                      of the meters that F4 has (a meter without F4 is reported, it cannot be billed)

Prints one table and OK or CHECK FAILED (exit code 1), so Airflow or the CI can stop on it.
"""

import argparse
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.clean_readings import clean_path, f1_paths
from jobs.control_counts import control_counts
from jobs.estimate_gaps import filled_path
from jobs.inventory import read_meters
from jobs.lake import Lake, add_lake_arguments, lake_from_args, read_text
from jobs.readings import read_f1, read_f3
from jobs.spark import get_spark

KWH_TOLERANCE = 0.0001       # 0.01%: fact_consumption rounds each customer-band-month to 4 decimals


def read_manifest(spark: SparkSession, lake: Lake, run: str) -> dict:
    return json.loads(read_text(spark, f"{clean_path(lake)}/_runs/{run}.json"))


def read_raw(spark: SparkSession, lake: Lake, manifest: dict) -> DataFrame:
    source = manifest["input"]
    # A run made by Airflow in Docker saved its input as /lake/raw/...: point it at this lake's raw layer.
    if "/raw/" in source and not source.startswith(lake.raw_root):
        source = f"{lake.raw_root}/{source.split('/raw/', 1)[1]}"
    if manifest["source"] == "f3":
        return read_f3(spark, [source])
    mode = manifest["run"].split("-")[1]                 # f1-<mode>-<date>
    return read_f1(spark, f1_paths(source, mode))


def layer_counts(raw: DataFrame, clean_rows: DataFrame, filled_run: DataFrame, filled_months: DataFrame,
                 fact: DataFrame, meters: DataFrame) -> dict:
    """filled_run: filled rows of the run's dates; filled_months and fact: the whole months of the run."""
    c = control_counts(raw, clean_rows)
    valid_clean = clean_rows.filter(F.col("is_valid") & F.col("interval_start").isNotNull()).count()
    real_filled = filled_run.filter(~F.col("is_estimated")).count()
    in_f4 = filled_months.join(meters.select("meter_id"), "meter_id")
    f = in_f4.agg(F.count("*").alias("rows"), F.sum("energy_kwh").alias("kwh")).first()
    k = fact.agg(F.sum("intervals").alias("rows"), F.sum("energy_kwh").alias("kwh")).first()
    return {
        "raw_readings": c["raw_readings"], "raw_meter_intervals": c["raw_meter_intervals"],
        "clean_rows": c["clean_rows"], "lost_raw_to_clean": c["lost"], "duplicated_in_clean": c["duplicated"],
        "clean_valid": valid_clean, "filled_rows": filled_run.count(), "filled_real": real_filled,
        "filled_estimated": filled_run.filter(F.col("is_estimated")).count(),
        "filled_month_rows_in_f4": f.rows, "fact_intervals": int(k.rows or 0),
        "filled_month_kwh_in_f4": float(f.kwh or 0.0), "fact_kwh": float(k.kwh or 0.0),
        "meters_without_f4": filled_months.select("meter_id").distinct()
        .join(meters.select("meter_id"), "meter_id", "left_anti").count(),
    }


def failures(counts: dict) -> list[str]:
    problems = []
    if counts["lost_raw_to_clean"] or counts["duplicated_in_clean"]:
        problems.append("raw -> clean lost or duplicated readings")
    if counts["filled_real"] != counts["clean_valid"]:
        problems.append("clean -> filled: valid readings missing or added")
    if counts["fact_intervals"] != counts["filled_month_rows_in_f4"]:
        problems.append("filled -> curated: intervals differ")
    kwh = counts["filled_month_kwh_in_f4"]
    if abs(counts["fact_kwh"] - kwh) > KWH_TOLERANCE * max(kwh, 1.0):
        problems.append("filled -> curated: kWh differ")
    return problems


def run(spark: SparkSession, lake: Lake, run_name: str) -> dict:
    manifest = read_manifest(spark, lake, run_name)
    dates = manifest["dates"]
    months = sorted({d[:7] for d in dates})
    clean_rows = spark.read.parquet(clean_path(lake)).filter(F.col("date").isin(dates))
    filled = spark.read.parquet(filled_path(lake))
    month_of = F.date_format("date", "yyyy-MM")
    fact = spark.read.parquet(lake.curated("fact_consumption")).filter(F.col("month").isin(months))
    return layer_counts(read_raw(spark, lake, manifest), clean_rows, filled.filter(F.col("date").isin(dates)),
                        filled.filter(month_of.isin(months)), fact, read_meters(spark, lake))


def main() -> None:
    p = argparse.ArgumentParser(description="Control counts between layers of a cleaning run (US-41)")
    add_lake_arguments(p)
    p.add_argument("--run", required=True, help="name of the cleaning run, e.g. f1-full-2026-10-28")
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-layer-counts")
    counts = run(spark, lake, args.run)
    for name, value in counts.items():
        print(f"{name:>24}: {value:,.4f}" if isinstance(value, float) else f"{name:>24}: {value:,}")
    problems = failures(counts)
    print("OK: raw, clean, filled and curated agree" if not problems else "CHECK FAILED: " + "; ".join(problems))
    spark.stop()
    if problems:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
