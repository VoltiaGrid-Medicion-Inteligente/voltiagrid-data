"""Estimate missing readings (US-34, RN-03): full grid (T-34.1), interpolation (T-34.2), 4-week profile (T-34.3).

    python -m jobs.estimate_gaps --run f3-2026-11-02 --lake ../voltiagrid-api/lake
    python -m jobs.estimate_gaps --date 2013-04-01 --lake ../voltiagrid-api/lake

Output: clean/source=readings/dataset=interval_30min_filled/date=YYYY-MM-DD/, exactly 48 rows per meter and day:
the valid reading when there is one, otherwise an estimate flagged with its method.
"""

import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.clean_readings import clean_path
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark

INTERVALS_PER_DAY = 48
MAX_INTERPOLATED_INTERVALS = 4  # RN-03: gaps up to 2 hours
PROFILE_WEEKS = 4                # RN-03: same weekday of the last 4 weeks
FILLED_COLUMNS = ["meter_id", "interval_start", "energy_kwh", "is_estimated", "estimation_method",
                  "quality_flag", "date"]


def as_dates(days: list[str] | list[date]) -> list[date]:
    return sorted(d if isinstance(d, date) else date.fromisoformat(d) for d in days)


def interval_grid(meter_days: DataFrame) -> DataFrame:
    """meter_days: (meter_id, first_day, last_day) -> one row per meter and 30-minute interval in those days."""
    first = F.col("first_day").cast("timestamp_ntz")
    last = (F.col("last_day") + F.expr("INTERVAL 1 DAY")).cast("timestamp_ntz") - F.expr("INTERVAL 30 MINUTES")
    return meter_days.select(
        "meter_id",
        F.explode(F.sequence(first, last, F.expr("INTERVAL 30 MINUTES"))).alias("interval_start"),
    )


def complete_grid(clean_rows: DataFrame, grid_from: date, grid_to: date) -> DataFrame:
    """Every interval of every meter between grid_from and grid_to, with its valid reading or a null.

    A meter gets a grid from the first day it has a reading with a valid clock (a new meter is not estimated before it
    existed; an invalid-clock reading has no interval and does not count).
    Columns: meter_id, interval_start, actual_kwh (null = gap), quality_flag ('missing' when there was no row at all).
    """
    seen = (clean_rows.filter(F.col("interval_start").isNotNull())
            .groupBy("meter_id").agg(F.min(F.to_date("interval_start")).alias("first_seen")))
    meter_days = seen.select(
        "meter_id",
        F.greatest(F.col("first_seen"), F.lit(grid_from)).alias("first_day"),
        F.lit(grid_to).alias("last_day"),
    ).filter(F.col("first_day") <= F.col("last_day"))
    readings = (
        clean_rows.filter(F.col("interval_start").isNotNull())
        .select("meter_id", "interval_start",
                F.when(F.col("is_valid"), F.col("energy_kwh")).alias("actual_kwh"), "quality_flag")
    )
    return (
        interval_grid(meter_days)
        .join(readings, ["meter_id", "interval_start"], "left")
        .withColumn("quality_flag", F.coalesce("quality_flag", F.lit("missing")))
    )


def interpolate(grid: DataFrame) -> DataFrame:
    """RN-03, first part (T-34.2): a gap of up to 4 intervals (2 hours) with a valid reading on both sides is filled
    on the straight line between them. Adds `interpolated_kwh` (null when the gap is longer or open at one end).

    Gaps-and-islands with windows: for each interval, the position and value of the last valid reading before it
    and the first after it. Gap length = next position - previous position - 1.
    """
    by_meter = Window.partitionBy("meter_id").orderBy("interval_start")
    before = by_meter.rowsBetween(Window.unboundedPreceding, 0)
    after = by_meter.rowsBetween(0, Window.unboundedFollowing)
    has_value = F.col("actual_kwh").isNotNull()
    positioned = (
        grid.withColumn("_pos", F.row_number().over(by_meter))
        .withColumn("_prev_pos", F.last(F.when(has_value, F.col("_pos")), ignorenulls=True).over(before))
        .withColumn("_prev_kwh", F.last("actual_kwh", ignorenulls=True).over(before))
        .withColumn("_next_pos", F.first(F.when(has_value, F.col("_pos")), ignorenulls=True).over(after))
        .withColumn("_next_kwh", F.first("actual_kwh", ignorenulls=True).over(after))
    )
    gap_length = F.col("_next_pos") - F.col("_prev_pos") - 1
    line = F.col("_prev_kwh") + (F.col("_next_kwh") - F.col("_prev_kwh")) * (
        (F.col("_pos") - F.col("_prev_pos")) / (F.col("_next_pos") - F.col("_prev_pos")))
    return positioned.withColumn(
        "interpolated_kwh",
        F.when(~has_value & (gap_length <= MAX_INTERPOLATED_INTERVALS), F.round(line, 4)),
    ).drop("_pos", "_prev_pos", "_prev_kwh", "_next_pos", "_next_kwh")


def weekday_profile(clean_rows: DataFrame) -> DataFrame:
    """RN-03, second part (T-34.3): for each meter and interval, the average of its valid readings at the same time of
    the same weekday in the previous 4 weeks (7, 14, 21 and 28 days before). Only real readings count, never estimates.

    Each valid reading is projected 1 to 4 weeks ahead and averaged where it lands, which is the same as looking back
    from every gap, without a self-join per gap. Columns: meter_id, interval_start, profile_kwh, profile_weeks.
    """
    valid = clean_rows.filter(F.col("is_valid") & F.col("interval_start").isNotNull())
    projected = valid.select(
        "meter_id", "energy_kwh",
        F.explode(F.sequence(F.lit(1), F.lit(PROFILE_WEEKS))).alias("_weeks"),
        "interval_start",
    ).withColumn("interval_start", F.col("interval_start") + F.make_interval(weeks=F.col("_weeks")))
    return projected.groupBy("meter_id", "interval_start").agg(
        F.round(F.avg("energy_kwh"), 4).alias("profile_kwh"),
        F.count("*").alias("profile_weeks"),
    )


def fill_gaps(clean_rows: DataFrame, days: list) -> DataFrame:
    """Rows for the given days (FILLED_COLUMNS). clean_rows must cover the days and the 28 days before them.

    The grid also covers the day before and the day after, so a gap that crosses midnight is measured and
    interpolated with the readings on both sides. Order of methods for a gap: interpolation (up to 2 hours, both
    neighbours known), then the 4-week profile, else 'unavailable' (no history: energy stays null, still estimated).
    """
    days = as_dates(days)
    grid = complete_grid(clean_rows, days[0] - timedelta(days=1), days[-1] + timedelta(days=1))
    filled = interpolate(grid).join(weekday_profile(clean_rows), ["meter_id", "interval_start"], "left")
    method = (
        F.when(F.col("actual_kwh").isNotNull(), F.lit(None))
        .when(F.col("interpolated_kwh").isNotNull(), F.lit("interpolation"))
        .when(F.col("profile_kwh").isNotNull(), F.lit("profile"))
        .otherwise(F.lit("unavailable"))
    )
    return (
        filled.withColumn("energy_kwh", F.coalesce("actual_kwh", "interpolated_kwh", "profile_kwh"))
        .withColumn("estimation_method", method)
        .withColumn("is_estimated", F.col("estimation_method").isNotNull())
        .withColumn("date", F.to_date("interval_start"))
        .filter(F.col("date").isin(days))
        .select(FILLED_COLUMNS)
    )


def history_window(days: list[date]) -> tuple[date, date]:
    """Clean dates a run needs: 28 days before the first day (profile, T-34.3) to the day after the last one."""
    return days[0] - timedelta(days=28), days[-1] + timedelta(days=1)


def filled_path(lake: Lake) -> str:
    return lake.clean("readings", "interval_30min_filled")


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    start, end = history_window(days)
    clean_rows = spark.read.parquet(clean_path(lake)).filter(F.col("date").between(start, end))
    result = fill_gaps(clean_rows, days)
    path = filled_path(lake)
    result.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")
    return spark.read.parquet(path).filter(F.col("date").isin(days))


def main() -> None:
    p = argparse.ArgumentParser(description="Estimate missing readings (US-34, RN-03)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-estimate-gaps")
    days = dates_from_args(spark, args, lake)
    started = datetime.now()
    result = run(spark, lake, days)
    methods = {r.estimation_method: r["count"] for r in result.groupBy("estimation_method").count().collect()}
    total = sum(methods.values())
    print(f"estimate_gaps: {total:,} rows in {len(days)} dates ({(datetime.now() - started).seconds} s); "
          f"methods {methods}")
    spark.stop()


if __name__ == "__main__":
    main()
