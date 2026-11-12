"""Control queries of the business questions (CT-10): the numbers the Power BI pages must show.

    python -m jobs.control_queries --question 2 --lake ../voltiagrid-api/lake --top 10
    python -m jobs.control_queries --question 3 --month 2013-01 --lake ../voltiagrid-api/lake --out control/q3
    python -m jobs.control_queries --question 5 --month 2013-01 --lake ../voltiagrid-api/lake

The queries follow docs/data/business-questions-matrix.md, with natural keys: the dimensions of jobs/dimensions.py have
no surrogate keys (band_key, customer_key, event_key), so transformer_code, customer_code, band_name and event_id join
the facts directly.

Question 1 has no ACORN group: dim_customer.acorn_group is not in F4 yet (open point of the dimensional model), so the
query returns band and tariff type only. Question 5 reads fact_load_weather_hourly (US-53, KAN-313), whose temperature
comes from F2.
"""

import argparse
import calendar
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.dimensions import customer_dim
from jobs.estimate_gaps import filled_path
from jobs.inventory import read_contracts, read_meters
from jobs.lake import Lake, add_lake_arguments, lake_from_args
from jobs.spark import get_spark


def month_bounds(month: str) -> tuple[date, date]:
    """First and last day of a month given as YYYY-MM."""
    year, mm = (int(x) for x in month.split("-"))
    if not 1 <= mm <= 12:
        raise ValueError(f"month must be YYYY-MM: {month}")
    return date(year, mm, 1), date(year, mm, calendar.monthrange(year, mm)[1])


def q1_consumption_by_band(spark: SparkSession, lake: Lake, month: str) -> DataFrame:
    """Q1: energy and amount by band and tariff type in one month. The tariff type is the one of the contract that is
    current on the first day of the month (dim_customer, type 2)."""
    first, _ = month_bounds(month)
    facts = spark.read.parquet(lake.curated("fact_consumption")).filter(F.col("month") == month)
    customers = customer_dim(read_contracts(spark, lake)).select(
        F.col("customer_code").alias("c_code"), "tariff_type", "valid_from", "valid_to")
    on_first_day = (F.col("c_code") == F.col("customer_code")) & \
        (F.col("valid_from") <= F.lit(first)) & (F.col("valid_to") >= F.lit(first))
    return (facts.join(customers, on_first_day, "left")
            .groupBy("band_name", "tariff_type")
            .agg(F.round(F.sum("energy_kwh"), 4).alias("kwh"), F.round(F.sum("amount"), 2).alias("amount"))
            .orderBy("band_name", "tariff_type"))


def q2_transformer_losses(spark: SparkSession, lake: Lake, top: int = 10) -> DataFrame:
    """Q2: losses per transformer over the whole fact, the top ones by loss cost."""
    losses = spark.read.parquet(lake.curated("fact_transformer_loss_daily"))
    return (losses.groupBy("transformer_code")
            .agg(F.round(F.sum("loss_kwh"), 4).alias("loss_kwh"),
                 F.round(F.sum("loss_cost"), 2).alias("loss_cost"),
                 F.round(F.avg("loss_pct"), 4).alias("avg_loss_pct"),
                 F.sum("is_suspicious").alias("suspicious_days"))
            .orderBy(F.col("loss_cost").desc(), "transformer_code")
            .limit(top))


def q3_estimated_readings(spark: SparkSession, lake: Lake, month: str) -> DataFrame:
    """Q3: share of estimated half hours per transformer in one month, from the filled readings and F4 (meter ->
    transformer). fact_consumption is by customer and band, so the question is answered from the readings."""
    first, last = month_bounds(month)
    readings = spark.read.parquet(filled_path(lake)).filter(F.col("date").between(F.lit(first), F.lit(last)))
    meters = read_meters(spark, lake).select("meter_id", "transformer_code").dropDuplicates(["meter_id"])
    return (readings.join(F.broadcast(meters), "meter_id")
            .groupBy("transformer_code")
            .agg(F.count("*").alias("intervals"),
                 F.sum(F.col("is_estimated").cast("int")).alias("estimated_intervals"))
            .withColumn("estimated_pct", F.round(100.0 * F.col("estimated_intervals") / F.col("intervals"), 2))
            .withColumn("month", F.lit(month))
            .orderBy("transformer_code"))


def q4_dr_savings(spark: SparkSession, lake: Lake) -> DataFrame:
    """Q4: savings and incentive cost per demand-response event."""
    facts = spark.read.parquet(lake.curated("fact_dr_participation"))
    return (facts.groupBy("event_id")
            .agg(F.round(F.sum("savings_kwh"), 4).alias("savings_kwh"),
                 F.sum("incentive_paid").alias("customers_paid"),
                 F.round(F.sum("incentive_amount"), 2).alias("incentive_cost"))
            .orderBy("event_id"))


def q5_temperature_and_peak_load(spark: SparkSession, lake: Lake, month: str) -> tuple[DataFrame, dict]:
    """Q5: temperature and peak load in one month. Returns the matrix query (highest load % of capacity and highest
    temperature per day) and the numbers of the page over the month's hourly rows: how many rows, the highest load %
    of capacity and the correlation between temperature and load % (Pearson, only hours that have a temperature)."""
    first, last = month_bounds(month)
    facts = spark.read.parquet(lake.curated("fact_load_weather_hourly")) \
        .filter(F.col("date").between(F.lit(first), F.lit(last)))
    by_day = (facts.groupBy("date")
              .agg(F.round(F.max("load_pct_capacity"), 4).alias("peak_load_pct"),
                   F.max("temperature_c").alias("max_temp_c"),
                   F.round(F.avg("temperature_c"), 2).alias("avg_temp_c"))
              .orderBy("date"))
    rows, peak = facts.agg(F.count("*"), F.max("load_pct_capacity")).first()
    corr = facts.filter(F.col("temperature_c").isNotNull()) \
        .agg(F.corr("temperature_c", "load_pct_capacity")).first()[0]
    totals = {"rows": rows,
              "peak_load_pct": None if peak is None else round(peak, 4),
              "correlation": None if corr is None else round(corr, 3)}
    return by_day, totals


def q6_customers_not_billed(spark: SparkSession, lake: Lake, month: str) -> tuple[DataFrame, int, int]:
    """Q6: customers left out of automatic billing in one month, by reason. Returns the table by reason and the two
    counts that must agree: inspection orders, and billing_status rows with is_billable = false."""
    orders = spark.read.parquet(lake.curated("inspection_orders")).filter(F.col("month") == month)
    status = spark.read.parquet(lake.curated("billing_status")).filter(F.col("month") == month)
    by_reason = orders.groupBy("reason").agg(F.countDistinct("customer_code").alias("customers"))
    return by_reason, orders.count(), status.filter(~F.col("is_billable")).count()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_lake_arguments(p)
    p.add_argument("--question", type=int, required=True, choices=[1, 2, 3, 4, 5, 6])
    p.add_argument("--month", help="YYYY-MM, for questions 1, 3, 5 and 6")
    p.add_argument("--top", type=int, default=10, help="for question 2")
    p.add_argument("--out", help="folder for a CSV with the result (optional)")
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-control-queries", ui=False)

    if args.question == 1:
        result = q1_consumption_by_band(spark, lake, args.month)
    elif args.question == 2:
        result = q2_transformer_losses(spark, lake, args.top)
    elif args.question == 3:
        result = q3_estimated_readings(spark, lake, args.month)
    elif args.question == 4:
        result = q4_dr_savings(spark, lake)
    elif args.question == 5:
        result, totals = q5_temperature_and_peak_load(spark, lake, args.month)
        print(f"question 5, {args.month}: {totals['rows']} rows, highest load {totals['peak_load_pct']} of capacity, "
              f"correlation temperature-load {totals['correlation']}")
    else:
        result, orders, not_billable = q6_customers_not_billed(spark, lake, args.month)
        if orders != not_billable:
            raise SystemExit(f"question 6: {orders} inspection orders but {not_billable} customers not billable")
    result.show(truncate=False)
    if args.out:
        result.coalesce(1).write.mode("overwrite").option("header", True).csv(args.out)
    spark.stop()


if __name__ == "__main__":
    main()
