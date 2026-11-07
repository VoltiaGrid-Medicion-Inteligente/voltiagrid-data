"""Demand-response events per circuit and day (US-43, T-43.1, RN-08) and who is notified (T-43.2, RN-10).

    python -m jobs.dr_events --date 2013-07-22 --lake ../voltiagrid-api/lake
    python -m jobs.dr_events --run f3-2026-11-02 --lake ../voltiagrid-api/lake

For each circuit and day D:
- forecast temperature: the highest F2 temperature of D inside the peak band (18:00 to 21:59). The pilot replays
  history, so the F2 value of the day plays the role of the forecast;
- projected load: the circuit load of the same peak intervals one week before (D - 7, curated circuit_load, US-33),
  a weekly persistence forecast. Its highest value is the projected load; the first interval above 85% is when the
  event starts.
Both above their thresholds (pipeline.demand_response, RN-08) -> one event, inside the peak band and 3 hours at most.

Participants (T-43.2): the customers of the event circuit whose contract on D has the dynamic tariff (the time-of-use
customers enrolled in the programme). Each one gets one notification in the outbox, the message a mailer sends.
RN-10 (T-44.3): a customer who already took part in 8 events this month is left out of the next ones.

Output, partitioned by date (a day without events keeps empty partitions with _SUCCESS):
    curated/dataset=dr_events/          one row per event
    curated/dataset=dr_participants/    one row per event and customer
    curated/dataset=dr_notifications/   one row per message to send
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.estimate_gaps import as_dates
from jobs.inventory import read_contracts, read_meters
from jobs.lake import (Lake, add_date_arguments, add_lake_arguments, dates_from_args, lake_from_args, path_exists,
                       write_text)
from jobs.spark import get_spark
from pipeline.demand_response import (LOAD_THRESHOLD, MAX_EVENTS_PER_MONTH, PEAK_END, PEAK_START, event_window,
                                      needs_event)
from schemas.f2 import F2_SCHEMA

EVENT_SCHEMA = ("event_id string, circuit_code string, start_ts timestamp_ntz, end_ts timestamp_ntz, "
                "duration_h double, forecast_temp_c double, projected_load_pct double, date date")
PARTICIPANT_SCHEMA = "event_id string, customer_code string, meter_id string, circuit_code string, date date"
NOTIFICATION_SCHEMA = "event_id string, customer_code string, channel string, message string, date date"
PARTICIPATING_TARIFF = "dynamic"
PROJECTION_DAYS = 7
CIRCUIT_LOAD_SCHEMA = "circuit_code string, interval_start timestamp_ntz, load_pct double, date date"


def events_path(lake: Lake) -> str:
    return lake.curated("dr_events")


def read_events(spark: SparkSession, lake: Lake) -> DataFrame:
    """Explicit schema: a dataset written only on days without events has no parquet file to infer it from."""
    return spark.read.schema(EVENT_SCHEMA).parquet(events_path(lake))


def read_participants(spark: SparkSession, lake: Lake) -> DataFrame:
    return spark.read.schema(PARTICIPANT_SCHEMA).parquet(lake.curated("dr_participants"))


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


def participants_of(events: DataFrame, meters: DataFrame, contracts: DataFrame) -> DataFrame:
    """T-43.2: customers of the event circuit with a dynamic-tariff contract valid on the event day."""
    valid = (F.col("start_date") <= F.col("date")) & (F.col("end_date").isNull() | (F.col("end_date") >= F.col("date")))
    return (
        events.select("event_id", "circuit_code", "date")
        .join(meters.select("meter_id", "customer_code", "circuit_code"), "circuit_code")
        .join(contracts.filter(F.col("tariff_type") == PARTICIPATING_TARIFF), "customer_code")
        .filter(valid)
        .select("event_id", "customer_code", "meter_id", "circuit_code", "date")
        .dropDuplicates(["event_id", "customer_code", "meter_id"])
    )


def within_monthly_limit(candidates: DataFrame, earlier: DataFrame, day: date) -> DataFrame:
    """RN-10 (T-44.3): drop the candidates who already took part in 8 events this month before `day`."""
    taken = (earlier.filter(F.col("date").between(F.lit(day.replace(day=1)), F.lit(day - timedelta(days=1))))
             .groupBy("customer_code").agg(F.countDistinct("event_id").alias("events_this_month")))
    return (candidates.join(taken, "customer_code", "left")
            .filter(F.coalesce("events_this_month", F.lit(0)) < MAX_EVENTS_PER_MONTH)
            .select(*candidates.columns))


def notifications_for(participants: DataFrame, events: DataFrame) -> DataFrame:
    """One message per customer and event (a customer with two meters on the circuit gets one)."""
    message = F.format_string(
        "Demand-response event on %s from %s to %s: please reduce your consumption. Saving 10%% or more against "
        "your usual consumption at those hours earns an incentive.",
        F.date_format("start_ts", "yyyy-MM-dd"), F.date_format("start_ts", "HH:mm"), F.date_format("end_ts", "HH:mm"))
    return (
        participants.select("event_id", "customer_code").distinct()
        .join(events.select("event_id", "start_ts", "end_ts", "date"), "event_id")
        .select("event_id", "customer_code", F.lit("email").alias("channel"), message.alias("message"), "date")
    )


def _write(spark: SparkSession, df: DataFrame, path: str, days: list[date]) -> None:
    df.repartition("date").write.mode("overwrite").partitionBy("date").parquet(path)
    for d in days:
        write_text(spark, f"{path}/date={d.isoformat()}/_SUCCESS", "")


def run(spark: SparkSession, lake: Lake, days: list) -> DataFrame:
    days = as_dates(days)
    circuit_load = spark.read.schema(CIRCUIT_LOAD_SCHEMA).parquet(lake.curated("circuit_load"))
    weather = read_weather(spark, lake)
    meters, contracts = read_meters(spark, lake), read_contracts(spark, lake)
    # participations of other days (a rerun of these days replaces their own rows): needed for RN-10
    earlier = (read_participants(spark, lake).filter(~F.col("date").isin(days))
               if path_exists(spark, lake.curated("dr_participants")) else spark.createDataFrame([], PARTICIPANT_SCHEMA))
    events = spark.createDataFrame([], EVENT_SCHEMA)
    participants = spark.createDataFrame([], PARTICIPANT_SCHEMA)
    for day in days:   # in date order: a participation of the 3rd counts for the limit on the 4th
        projections = projected_peak_load(circuit_load, day).collect()
        day_events = declare_day(spark, projections, peak_forecast(weather, day), day)
        candidates = participants_of(day_events, meters, contracts)
        allowed = within_monthly_limit(candidates, earlier.unionByName(participants), day)
        events = events.unionByName(day_events).localCheckpoint(eager=True)
        participants = participants.unionByName(allowed).localCheckpoint(eager=True)
    _write(spark, events, events_path(lake), days)
    _write(spark, participants, lake.curated("dr_participants"), days)
    _write(spark, notifications_for(participants, events), lake.curated("dr_notifications"), days)
    return events


def main() -> None:
    p = argparse.ArgumentParser(description="Demand-response events per circuit and day (US-43, RN-08)")
    add_lake_arguments(p)
    add_date_arguments(p)
    args = p.parse_args()
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-dr-events")
    days = dates_from_args(spark, args, lake)
    events = run(spark, lake, days).orderBy("date", "circuit_code").collect()
    notified = spark.read.schema(NOTIFICATION_SCHEMA).parquet(lake.curated("dr_notifications")) \
        .filter(F.col("date").isin(days)).count()
    print(f"dr_events: {len(events)} events in {len(days)} dates, {notified} customers notified")
    for e in events:
        print(f"  {e.event_id}: {e.start_ts:%H:%M}-{e.end_ts:%H:%M} ({e.duration_h:g} h), "
              f"forecast {e.forecast_temp_c:.1f} °C, projected load {e.projected_load_pct:.1%}")
    spark.stop()


if __name__ == "__main__":
    main()
