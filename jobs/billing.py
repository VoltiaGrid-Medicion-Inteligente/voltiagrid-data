"""Monthly billing check (US-36, RN-04): estimated share (T-36.1) and inspection orders (T-36.2).

    python -m jobs.billing --month 2013-01 --lake ../voltiagrid-api/lake
    python -m jobs.billing --run f3-2026-11-02 --lake ../voltiagrid-api/lake

Input: curated/dataset=fact_consumption (US-35), which already counts intervals and estimated intervals per band.
Output:
    curated/dataset=billing_status/month=YYYY-MM/     every customer: estimated_pct, is_billable, inspection_order
    curated/dataset=inspection_orders/month=YYYY-MM/  only customers not billed: one order with reason and percentage
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.lake import Lake, add_lake_arguments, add_month_arguments, lake_from_args, months_from_args, write_text
from jobs.spark import get_spark

MAX_ESTIMATED_PCT = 0.10  # RN-04: more than 10% estimated -> not billable
REASON = "estimated readings above 10% of the month (RN-04)"


def estimated_share(fact: DataFrame) -> DataFrame:
    """One row per customer and month: intervals, estimated intervals and estimated_pct (0.12 = 12%).

    It counts intervals, not kWh: RN-04 talks about estimated readings. Intervals marked 'unavailable' (no data to
    estimate from) count as estimated.
    """
    return (
        fact.groupBy("customer_code", "month")
        .agg(F.sum("intervals").alias("intervals"), F.sum("estimated_intervals").alias("estimated_intervals"))
        .withColumn("estimated_pct", F.round(F.col("estimated_intervals") / F.col("intervals"), 4))
    )


def close_month(share: DataFrame) -> DataFrame:
    """RN-04 for each customer and month. Exactly 10% is still billable; above it, an inspection order instead."""
    over = F.col("estimated_pct") > MAX_ESTIMATED_PCT
    return share.withColumn("inspection_order", over).withColumn("is_billable", ~over)


def inspection_orders(status: DataFrame, created_at: datetime) -> DataFrame:
    """One order per customer and month that cannot be billed. The id is stable: closing the month again rewrites the
    same order instead of opening a second one."""
    return (
        status.filter(F.col("inspection_order"))
        .select(
            F.concat_ws("-", F.lit("IO"), F.col("month"), F.col("customer_code")).alias("order_id"),
            "customer_code", "month", "estimated_pct", "estimated_intervals", "intervals",
            F.lit(REASON).alias("reason"),
            # from text: a Python datetime literal would be shifted by the machine's time zone
            F.lit(created_at.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz").alias("created_at"),
        )
    )


def _write(spark: SparkSession, df: DataFrame, path: str, months: list[str]) -> None:
    df.repartition("month").write.mode("overwrite").partitionBy("month").parquet(path)
    for m in months:
        write_text(spark, f"{path}/month={m}/_SUCCESS", "")


def run(spark: SparkSession, lake: Lake, months: list[str]) -> tuple[DataFrame, DataFrame]:
    fact = spark.read.parquet(lake.curated("fact_consumption")).filter(F.col("month").isin(months))
    status = close_month(estimated_share(fact)).localCheckpoint(eager=True)
    orders = inspection_orders(status, datetime.now(timezone.utc))
    _write(spark, status, lake.curated("billing_status"), months)
    _write(spark, orders, lake.curated("inspection_orders"), months)
    return status, orders


def main() -> None:
    p = argparse.ArgumentParser(description="Billing check and inspection orders (US-36, RN-04)")
    add_lake_arguments(p)
    add_month_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-billing")
    months = months_from_args(spark, args, lake)
    status, orders = run(spark, lake, months)
    total, blocked = status.count(), orders.count()
    print(f"billing: {total:,} customer-months, {total - blocked:,} billable, {blocked:,} inspection orders "
          f"in {', '.join(months)}")
    spark.stop()


if __name__ == "__main__":
    main()
