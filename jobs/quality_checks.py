"""Quality checks of the lake after a run: counts, nulls and ranges (US-49, T-49.3).

    python -m jobs.quality_checks --run f3-2026-11-02 --lake ../voltiagrid-api/lake

For the dates of a cleaning run (and their months) it checks clean, filled, circuit_load, fact_consumption and
billing_status:
- counts: every dataset has rows for the run, and every meter-day of filled has its 48 intervals;
- nulls and ranges: the conditions of pipeline/quality.py, counted as bad rows.
Prints one line per check and `OK: N quality checks passed`, or `QUALITY FAILED` and exit code 1, so the CI (the
pipeline-smoke job) and the DAG stop instead of publishing bad data.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.clean_readings import clean_path
from jobs.estimate_gaps import filled_path
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args
from jobs.spark import get_spark
from pipeline.quality import INTERVALS_PER_DAY, ROW_CHECKS


def datasets(spark: SparkSession, lake: Lake, dates: list[str]) -> dict[str, DataFrame]:
    months = sorted({d[:7] for d in dates})
    by_date = lambda path: spark.read.parquet(path).filter(F.col("date").isin(dates))   # noqa: E731
    by_month = lambda path: spark.read.parquet(path).filter(F.col("month").isin(months))   # noqa: E731
    return {
        "clean": by_date(clean_path(lake)),
        "filled": by_date(filled_path(lake)),
        "circuit_load": by_date(lake.curated("circuit_load")),
        "fact_consumption": by_month(lake.curated("fact_consumption")),
        "billing_status": by_month(lake.curated("billing_status")),
    }


def run_checks(data: dict[str, DataFrame]) -> list[tuple[str, str, int]]:
    """(dataset, check, bad rows) for every check; one aggregation per dataset."""
    results = []
    for name, df in data.items():
        checks = ROW_CHECKS[name]
        row = df.agg(F.count("*").alias("_rows"),
                     *[F.sum(F.expr(cond).cast("int")).alias(f"_c{i}") for i, (_, cond) in enumerate(checks)]).first()
        results.append((name, "has rows for the run", 0 if row["_rows"] else 1))
        results += [(name, check, int(row[f"_c{i}"] or 0)) for i, (check, _) in enumerate(checks)]
    incomplete = (data["filled"].groupBy("meter_id", "date").count()
                  .filter(F.col("count") != INTERVALS_PER_DAY).count())
    results.append(("filled", f"{INTERVALS_PER_DAY} intervals per meter and day", incomplete))
    return results


def main() -> None:
    p = argparse.ArgumentParser(description="Quality checks of the lake after a run (US-49)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-quality-checks")
    dates = dates_from_args(spark, args, lake)
    results = run_checks(datasets(spark, lake, dates))
    for name, check, bad in results:
        print(f"{'ok  ' if bad == 0 else 'FAIL'} {name:<17} {check:<45} {bad:>8,} bad")
    failed = [r for r in results if r[2]]
    print(f"OK: {len(results)} quality checks passed" if not failed else f"QUALITY FAILED: {len(failed)} checks")
    spark.stop()
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
