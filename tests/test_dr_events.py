"""Demand-response events per circuit and day (US-43, T-43.1) and their participants and notifications (T-43.2)."""

from datetime import date, datetime, timedelta
from decimal import Decimal

from jobs.lake import Lake

DAY = date(2013, 7, 22)                 # a Monday
WEEK_BEFORE = DAY - timedelta(days=7)
HALF_HOUR = timedelta(minutes=30)


def write_circuit_load(spark, lake, loads):
    """loads: {circuit: {"HH:MM": load_pct}} for D - 7; every other interval of that day is at 50%."""
    rows = []
    for circuit, overrides in loads.items():
        ts = datetime.combine(WEEK_BEFORE, datetime.min.time())
        for _ in range(48):
            rows.append((circuit, ts, overrides.get(ts.strftime("%H:%M"), 0.5), WEEK_BEFORE))
            ts += HALF_HOUR
    spark.createDataFrame(rows, "circuit_code string, interval_start timestamp_ntz, load_pct double, date date") \
        .write.mode("overwrite").partitionBy("date").parquet(lake.curated("circuit_load"))


def write_weather(lake, temps):
    """temps: {"HH:MM": temperature} for D; every other hour of the day is at 20 °C."""
    folder = f"{lake.raw('f2', 'weather_hourly')}/ingest_date=2026-11-01"
    lines = ["visibility,windBearing,temperature,time,dewPoint,pressure,apparentTemperature,windSpeed,precipType,"
             "icon,humidity,summary"]
    for hour in range(24):
        hh = f"{hour:02d}:00"
        lines.append(f"10,200,{temps.get(hh, 20.0)},{DAY} {hh}:00,10,1010,20,3,rain,clear-day,0.5,Clear")
    from pathlib import Path

    Path(folder).mkdir(parents=True, exist_ok=True)
    Path(folder, "weather_hourly_darksky.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_inventory(lake):
    """CT-1: M1 (dynamic), M2 (standard), M3 and M4 of the same dynamic customer; M5 on CT-2 (dynamic).
    CU-6 is dynamic only from August, so on DAY it is standard."""
    from jobs.inventory import write_raw

    meters = [("M1", "CU-1", "CT-1"), ("M2", "CU-2", "CT-1"), ("M3", "CU-3", "CT-1"), ("M4", "CU-3", "CT-1"),
              ("M5", "CU-5", "CT-2"), ("M6", "CU-6", "CT-1")]
    contracts = [("CU-1", "dynamic", date(2011, 11, 1), None), ("CU-2", "standard", date(2011, 11, 1), None),
                 ("CU-3", "dynamic", date(2011, 11, 1), None), ("CU-5", "dynamic", date(2011, 11, 1), None),
                 ("CU-6", "standard", date(2011, 11, 1), date(2013, 7, 31)), ("CU-6", "dynamic", date(2013, 8, 1), None)]
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"],
                   [(m, cu, f"TR-{ct}", Decimal("100"), ct, Decimal("1000")) for m, cu, ct in meters]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"],
                      [(cu, f"CT-{cu}-{i}", t, start, end) for i, (cu, t, start, end) in enumerate(contracts)]),
    }, date(2026, 11, 1))


def build_lake(spark, tmp_path, loads, temps):
    lake = Lake.local(str(tmp_path / "lake"))
    write_circuit_load(spark, lake, loads)
    write_weather(lake, temps)
    write_inventory(lake)
    return lake


def test_hot_day_and_overloaded_circuit_declare_one_event_from_the_first_overloaded_interval(spark, tmp_path):
    from jobs.dr_events import run

    lake = build_lake(spark, tmp_path, {"CT-1": {"19:00": 0.9, "19:30": 0.95}, "CT-2": {"19:00": 0.8}},
                      {"18:00": 31.0, "19:00": 32.5})
    events = run(spark, lake, [DAY]).collect()
    assert [e.circuit_code for e in events] == ["CT-1"]        # CT-2 stays at 80%
    e = events[0]
    assert e.event_id == "DR-CT-1-20130722"
    assert (e.start_ts, e.end_ts, e.duration_h) == (datetime(2013, 7, 22, 19, 0), datetime(2013, 7, 22, 22, 0), 3.0)
    assert (e.forecast_temp_c, e.projected_load_pct) == (32.5, 0.95)


def test_overloaded_circuit_on_a_mild_day_declares_nothing(spark, tmp_path):
    from jobs.dr_events import read_events, run

    lake = build_lake(spark, tmp_path, {"CT-1": {"19:00": 0.95}}, {"19:00": 29.0})
    assert run(spark, lake, [DAY]).count() == 0
    assert read_events(spark, lake).count() == 0               # empty partition, still readable


def test_heat_outside_the_peak_band_does_not_count(spark, tmp_path):
    from jobs.dr_events import run

    lake = build_lake(spark, tmp_path, {"CT-1": {"19:00": 0.95}}, {"15:00": 34.0})
    assert run(spark, lake, [DAY]).count() == 0


def test_rerunning_the_day_gives_the_same_events(spark, tmp_path):
    from jobs.dr_events import read_events, run

    lake = build_lake(spark, tmp_path, {"CT-1": {"18:00": 0.9}}, {"18:00": 31.0})
    run(spark, lake, [DAY])
    run(spark, lake, [DAY])
    rows = read_events(spark, lake).collect()
    assert [(r.event_id, r.start_ts.time().isoformat()) for r in rows] == [("DR-CT-1-20130722", "18:00:00")]


# ---------- T-43.2: participants and notifications ----------

def test_only_dynamic_tariff_customers_of_the_event_circuit_take_part(spark, tmp_path):
    from jobs.dr_events import read_participants, run

    lake = build_lake(spark, tmp_path, {"CT-1": {"19:00": 0.95}, "CT-2": {"19:00": 0.5}}, {"19:00": 31.0})
    run(spark, lake, [DAY])
    rows = sorted((r.customer_code, r.meter_id) for r in read_participants(spark, lake).collect())
    assert rows == [("CU-1", "M1"), ("CU-3", "M3"), ("CU-3", "M4")]   # CU-2 standard, CU-5 on CT-2, CU-6 not yet


def test_each_participating_customer_gets_one_message_with_the_event_window(spark, tmp_path):
    from jobs.dr_events import NOTIFICATION_SCHEMA, run

    lake = build_lake(spark, tmp_path, {"CT-1": {"19:00": 0.95}}, {"19:00": 31.0})
    run(spark, lake, [DAY])
    rows = spark.read.schema(NOTIFICATION_SCHEMA).parquet(lake.curated("dr_notifications")).collect()
    assert sorted(r.customer_code for r in rows) == ["CU-1", "CU-3"]
    assert {r.channel for r in rows} == {"email"}
    assert rows[0].message.startswith("Demand-response event on 2013-07-22 from 19:00 to 22:00")


def test_a_day_without_events_notifies_nobody(spark, tmp_path):
    from jobs.dr_events import read_participants, run

    lake = build_lake(spark, tmp_path, {"CT-1": {"19:00": 0.95}}, {"19:00": 25.0})
    run(spark, lake, [DAY])
    assert read_participants(spark, lake).count() == 0
