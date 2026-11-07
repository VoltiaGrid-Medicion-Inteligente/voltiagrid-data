"""Demand-response events per circuit and day (US-43, T-43.1)."""

from datetime import date, datetime, timedelta

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


def build_lake(spark, tmp_path, loads, temps):
    lake = Lake.local(str(tmp_path / "lake"))
    write_circuit_load(spark, lake, loads)
    write_weather(lake, temps)
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
