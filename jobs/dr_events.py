"""Demand-response events per circuit and day (US-43, T-43.1, RN-08) -> curated dr_events.

    python -m jobs.dr_events --date 2013-07-22 --lake ../voltiagrid-api/lake
    python -m jobs.dr_events --run f3-2026-11-02 --lake ../voltiagrid-api/lake

For each circuit and day D:
- forecast temperature: the highest F2 temperature of D inside the peak band (18:00 to 21:59). The pilot replays
  history, so the F2 value of the day plays the role of the forecast;
- projected load: the circuit load of the same peak intervals one week before (D - 7, curated circuit_load, US-33),
  a weekly persistence forecast. Its highest value is the projected load; the first interval above 85% is when the
  event starts.
Both above their thresholds (pipeline.demand_response, RN-08) -> one event, inside the peak band and 3 hours at most.

Output: curated/dataset=dr_events/date=YYYY-MM-DD/ (a day without events keeps an empty partition with _SUCCESS).
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.estimate_gaps import as_dates
from jobs.lake import Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, write_text
from jobs.spark import get_spark
from pipeline.demand_response import LOAD_THRESHOLD, PEAK_END, PEAK_START, event_window, needs_event
from schemas.f2 import F2_SCHEMA

EVENT_SCHEMA = ("event_id string, circuit_code string, start_ts timestamp_ntz, end_ts timestamp_ntz, "
                "duration_h double, forecast_temp_c double, projected_load_pct double, date date")
PROJECTION_DAYS = 7
CIRCUIT_LOAD_SCHEMA = "circuit_code string, interval_start timestamp_ntz, load_pct double, date date"


def events_path(lake: Lake) -> str:
    return lake.curated("dr_events")


def read_events(spark: SparkSession, lake: Lake) -> DataFrame:
    """Explicit schema: a dataset written only on days without events has no parquet file to infer it from."""
    return spark.read.schema(EVENT_SCHEMA).parquet(events_path(lake))


def in_peak(ts) -> F.Column:
    t = F.date_format(ts, "HH:mm:ss")
    return (t >= F.lit(PEAK_START.strftime("%H:%M:%S"))) & (t < F.lit(PEAK_END.strftime("%H:%M:%S")))


def read_weather(spark: SparkSession, lake: Lake) -> DataFrame:
    path = f"{lake.raw('f2', 'weather_hourly')}/ingest_date=*/*.csv"
    return spark.read.csv(path, header=True, schema=F2_SCHEMA, timestampFormat="yyyy-MM-dd HH:mm:ss")


def peak_forecast(weather: DataFrame, day: date) -> float | None:
    """Highest temperature of the day inside the peak band (None when F2 has no hour for it)."""
    row = (weather.filter((F.to_date("time") == F.lit(day)) & in_peak(F.col("time")))
           .agg(F.max("temperature").alias("t")).first())
    return row.t if row else None


def projected_peak_load(circuit_load: DataFrame, day: date) -> DataFrame:
    """Per circuit: highest peak load of D - 7 and the time of its first peak interval above the threshold."""
    week_before = day - timedelta(days=PROJECTION_DAYS)
    peak = circuit_load.filter((F.col("date") == F.lit(week_before)) & in_peak(F.col("interval_start")))
    first_over = F.min(F.when(F.col("load_pct") > LOAD_THRESHOLD, F.col("interval_start")))
    return peak.groupBy("circuit_code").agg(F.max("load_pct").alias("projected_load_pct"),
                                            first_over.alias("first_over"))


def declare_day(spark: SparkSession, projections: list, forecast_temp_c: float | None, day: date) -> DataFrame:
    """RN-08 per circuit with the functions of pipeline.demand_response (one row per circuit: Python is enough)."""
    rows = []
    for p in sorted(projections, key=lambda r: r.circuit_code):
        if not needs_event(forecast_temp_c, p.projected_load_pct):
            continue
        start, end = event_window(day, p.first_over.time() if p.first_over else PEAK_START)
        rows.append((f"DR-{p.circuit_code}-{day:%Y%m%d}", p.circuit_code, start, end,
                     (end - start).total_seconds() / 3600, float(forecast_temp_c), float(p.projected_load_pct), day))
    return spark.createDataFrame(rows, EVENT_SCHEMA)


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    circuit_load = spark.read.schema(CIRCUIT_LOAD_SCHEMA).parquet(lake.curated("circuit_load"))
    weather = read_weather(spark, lake)
    result = spark.createDataFrame([], EVENT_SCHEMA)
    for day in days:
        projections = projected_peak_load(circuit_load, day).collect()
        result = result.unionByName(declare_day(spark, projections, peak_forecast(weather, day), day))
    result = result.localCheckpoint(eager=True)
    path = events_path(lake)
    result.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")
    return result


def main() -> None:
    p = argparse.ArgumentParser(description="Demand-response events per circuit and day (US-43, RN-08)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-dr-events")
    days = dates_from_args(spark, args, lake)
    events = run(spark, lake, days).orderBy("date", "circuit_code").collect()
    print(f"dr_events: {len(events)} events in {len(days)} dates")
    for e in events:
        print(f"  {e.event_id}: {e.start_ts:%H:%M}-{e.end_ts:%H:%M} ({e.duration_h:g} h), "
              f"forecast {e.forecast_temp_c:.1f} °C, projected load {e.projected_load_pct:.1%}")
    spark.stop()


if __name__ == "__main__":
    main()
