"""Checks on the clean readings, for the job review and after any big run (US-30, T-30.6).

    python -m jobs.clean_report --lake ../voltiagrid-api/lake
    python -m jobs.clean_report --lake ../voltiagrid-api/lake --from 2013-01-01 --to 2013-01-31

Prints, as Markdown tables ready to paste in docs/data/clean-job-review.md:
- rows per quality flag and source, and the share of valid readings;
- duplicates left (two rows for the same meter and interval): must be 0;
- dates, meters and intervals covered.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.clean_readings import clean_path
from jobs.lake import add_lake_arguments, lake_from_args
from jobs.spark import get_spark


def report(df: DataFrame) -> dict:
    flags = {(r.source, r.quality_flag): r["count"] for r in df.groupBy("source", "quality_flag").count().collect()}
    total = sum(flags.values())
    valid = sum(n for (_, flag), n in flags.items() if flag == "ok")
    duplicates = (
        df.filter(F.col("interval_start").isNotNull())
        .groupBy("meter_id", "interval_start").count()
        .filter(F.col("count") > 1).count()
    )
    span = df.agg(F.min("date").alias("first"), F.max("date").alias("last"),
                  F.countDistinct("date").alias("dates"), F.countDistinct("meter_id").alias("meters")).first()
    return {"flags": flags, "rows": total, "valid_share": valid / total if total else 0.0,
            "duplicates": duplicates, "first": span["first"], "last": span["last"],
            "dates": span["dates"], "meters": span["meters"]}


def as_markdown(r: dict) -> str:
    lines = ["| Source | Flag | Rows | Share |", "|---|---|---|---|"]
    for (source, flag), n in sorted(r["flags"].items(), key=lambda kv: (kv[0][0], -kv[1])):
        lines.append(f"| {source} | `{flag}` | {n:,} | {n / r['rows']:.2%} |")
    lines += [
        "",
        "| Check | Value |",
        "|---|---|",
        f"| Rows | {r['rows']:,} |",
        f"| Valid readings | {r['valid_share']:.2%} |",
        f"| Duplicates (meter + interval) | {r['duplicates']:,} |",
        f"| Dates | {r['dates']:,} ({r['first']} to {r['last']}) |",
        f"| Meters | {r['meters']:,} |",
    ]
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description="Checks on the clean readings")
    add_lake_arguments(p)
    p.add_argument("--from", dest="date_from", default=None)
    p.add_argument("--to", dest="date_to", default=None)
    args = p.parse_args()
    spark = get_spark("voltiagrid-clean-report")
    df = spark.read.parquet(clean_path(lake_from_args(args)))
    if args.date_from:
        df = df.filter(F.col("date") >= args.date_from)
    if args.date_to:
        df = df.filter(F.col("date") <= args.date_to)
    print(as_markdown(report(df)))
    spark.stop()


if __name__ == "__main__":
    main()
