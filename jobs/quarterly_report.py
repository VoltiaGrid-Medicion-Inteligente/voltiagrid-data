"""Quarterly report on losses and metering quality (US-54, T-54.1).

    python -m jobs.quarterly_report --quarter 2013-Q1 --lake ../voltiagrid-api/lake

One row per transformer for the quarter, built from what the daily jobs already wrote:
- losses (curated fact_transformer_loss_daily, US-51): delivered, metered and lost kWh, the loss in money (T-51.2)
  and loss_pct = loss_kwh / delivered_kwh;
- metering quality (the filled half hours, US-34): how many readings were real, estimated (a gap) and invalid (a
  flagged reading, estimated like a gap), and each one's share of the total.

The figures are checked against the losses fact before anything is written (T-54.3): if they differ, the job stops.
Output: curated/dataset=quarterly_report_transformers/quarter=2013-Q1/   parquet
        curated/dataset=quarterly_report_csv/quarter=2013-Q1/           CSV with a header row (one part file)
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.estimate_gaps import filled_path
from jobs.inventory import read_meters
from jobs.lake import Lake, add_lake_arguments, lake_from_args
from jobs.spark import get_spark

REPORT_COLUMNS = ["quarter", "transformer_code", "circuit_code", "delivered_kwh", "metered_kwh", "loss_kwh", "loss_pct",
                  "loss_cost", "real_readings", "estimated_readings", "invalid_readings", "share_real",
                  "share_estimated", "share_invalid"]
MONTHS_OF_QUARTER = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}


def quarter_arg(text: str) -> str:
    """Argparse type: '2013-Q1' is valid, anything else is not."""
    try:
        year, q = text.split("-Q")
        int(year)
        if int(q) not in MONTHS_OF_QUARTER:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError(f"quarter must look like 2013-Q1, got {text!r}")
    return text


def quarter_days(quarter: str) -> list[date]:
    """Every day of a quarter: 2013-Q1 is 1 January to 31 March 2013."""
    year, q = quarter.split("-Q")
    first_month, last_month = MONTHS_OF_QUARTER[int(q)]
    start = date(int(year), first_month, 1)
    after = date(int(year) + (last_month == 12), last_month % 12 + 1, 1)
    return [start + timedelta(days=i) for i in range((after - start).days)]


def losses_by_transformer(fact: DataFrame) -> DataFrame:
    """The quarter's losses per transformer, summed from the daily rows of fact_transformer_loss_daily."""
    return fact.groupBy("transformer_code").agg(
        F.sum("delivered_kwh").alias("delivered_kwh"),
        F.sum("metered_kwh").alias("metered_kwh"),
        F.sum("loss_kwh").alias("loss_kwh"),
        F.sum("loss_cost").alias("loss_cost"),
    )


def reading_quality(spark: SparkSession, lake: Lake, days: list[date], meters: DataFrame) -> DataFrame:
    """Counts of real, estimated and invalid readings per transformer over the quarter's filled half hours."""
    filled = spark.read.parquet(filled_path(lake)).filter(F.col("date").isin(days))
    kind = (F.when(~F.col("is_estimated"), "real")
            .when(F.col("quality_flag") == "missing", "estimated")
            .otherwise("invalid"))
    return (
        filled.withColumn("kind", kind)
        .join(F.broadcast(meters.select("meter_id", "transformer_code")), "meter_id")
        .groupBy("transformer_code")
        .agg(
            F.sum(F.when(F.col("kind") == "real", 1).otherwise(0)).alias("real_readings"),
            F.sum(F.when(F.col("kind") == "estimated", 1).otherwise(0)).alias("estimated_readings"),
            F.sum(F.when(F.col("kind") == "invalid", 1).otherwise(0)).alias("invalid_readings"),
        )
    )


def build_report(spark: SparkSession, lake: Lake, quarter: str, fact: DataFrame) -> DataFrame:
    days = quarter_days(quarter)
    meters = read_meters(spark, lake)
    total = F.col("real_readings") + F.col("estimated_readings") + F.col("invalid_readings")
    report = (
        losses_by_transformer(fact)
        .join(reading_quality(spark, lake, days, meters), "transformer_code", "left")
        .join(meters.select("transformer_code", "circuit_code").distinct(), "transformer_code", "left")
        .fillna(0, ["real_readings", "estimated_readings", "invalid_readings"])
        .withColumn("quarter", F.lit(quarter))
        .withColumn("loss_pct", F.when(F.col("delivered_kwh") > 0, F.col("loss_kwh") / F.col("delivered_kwh")))
        .withColumn("share_real", F.when(total > 0, F.col("real_readings") / total))
        .withColumn("share_estimated", F.when(total > 0, F.col("estimated_readings") / total))
        .withColumn("share_invalid", F.when(total > 0, F.col("invalid_readings") / total))
    )
    return report.select(REPORT_COLUMNS)


def check_against_fact(report: DataFrame, fact: DataFrame) -> list[str]:
    """Transformers whose loss_kwh in the report differs from the sum of the losses fact, or that are missing on one
    side. An empty list means the figures match (T-54.3)."""
    expected = fact.groupBy("transformer_code").agg(F.sum("loss_kwh").alias("expected_kwh"))
    actual = report.select("transformer_code", F.col("loss_kwh").alias("reported_kwh"))
    joined = actual.join(expected, "transformer_code", "full_outer")
    bad = joined.filter(
        F.col("reported_kwh").isNull() | F.col("expected_kwh").isNull()
        | (F.abs(F.col("reported_kwh") - F.col("expected_kwh")) > 1e-6)
    )
    return sorted(r.transformer_code for r in bad.select("transformer_code").collect())


def write_report(report: DataFrame, lake: Lake, quarter: str) -> None:
    report.write.mode("overwrite").parquet(f"{lake.curated('quarterly_report_transformers')}/quarter={quarter}")
    (report.coalesce(1).write.mode("overwrite").option("header", True)
     .csv(f"{lake.curated('quarterly_report_csv')}/quarter={quarter}"))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_lake_arguments(p)
    p.add_argument("--quarter", required=True, type=quarter_arg, help="quarter to report, e.g. 2013-Q1")
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-quarterly-report", ui=False)
    days = quarter_days(args.quarter)
    fact = spark.read.parquet(lake.curated("fact_transformer_loss_daily")).filter(F.col("date").isin(days))
    if fact.limit(1).count() == 0:
        raise SystemExit(f"quarterly report: no losses in the lake for {args.quarter}; run the daily jobs first")
    report = build_report(spark, lake, args.quarter, fact)
    mismatches = check_against_fact(report, fact)
    if mismatches:
        raise SystemExit(f"quarterly report: loss figures differ from the fact for {mismatches[:10]}")
    write_report(report, lake, args.quarter)
    print(f"quarterly report {args.quarter}: {report.count()} transformers written")
    spark.stop()


if __name__ == "__main__":
    main()
