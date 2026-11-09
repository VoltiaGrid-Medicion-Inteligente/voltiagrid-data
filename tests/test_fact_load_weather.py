"""Load and weather fact per transformer and hour (US-53, KAN-313): measures, keys, weather join and incomplete hours."""

import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from jobs.lake import Lake

DAY = date(2013, 1, 20)


def write_f5(lake, transformer, quarters):
    """quarters: list of 96 kWh values for DAY (None = quarter missing from the F5 answer)."""
    from jobs.fetch_f5 import object_name
    from jobs.lake import put_object

    start = datetime.combine(DAY, datetime.min.time())
    readings = [{"interval_start": (start + i * timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "delivered_kwh": kwh} for i, kwh in enumerate(quarters) if kwh is not None]
    answer = {"transformer_code": transformer, "interval_minutes": 15, "readings": readings}
    put_object(f"{lake.raw('f5', 'transformer_energy')}/ingest_date=2026-11-01/{object_name(transformer, DAY)}",
               json.dumps(answer))


def write_weather(lake, temps):
    """temps: {hour: temperature} for DAY; hours not given have no F2 row."""
    folder = Path(f"{lake.raw('f2', 'weather_hourly')}/ingest_date={DAY}")
    folder.mkdir(parents=True, exist_ok=True)
    lines = ["visibility,windBearing,temperature,time,dewPoint,pressure,apparentTemperature,windSpeed,precipType,"
             "icon,humidity,summary"]
    lines += [f"10,200,{t},{DAY} {h:02d}:00:00,10,1010,{t},3,rain,clear-day,0.5,Clear" for h, t in sorted(temps.items())]
    (folder / "weather_hourly_darksky.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_lake(tmp_path, f5, temps, capacity_kva="100"):
    from jobs.inventory import write_raw

    lake = Lake.local(str(tmp_path / "lake"))
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"],
                   [(f"M-{t}", f"CU-{t}", t, Decimal(capacity_kva), "CT-1", Decimal("1000")) for t in f5]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"], []),
    }, date(2026, 11, 1))
    for t, quarters in f5.items():
        write_f5(lake, t, quarters)
    write_weather(lake, temps)
    return lake


def rows(spark, lake):
    from jobs.fact_load_weather import run

    return {(r.transformer_code, r.hour): r for r in run(spark, lake, [DAY]).collect()}


def test_one_row_per_transformer_and_hour_with_peak_load_and_keys(spark, tmp_path):
    quarters = [1.0] * 96
    quarters[18 * 4 + 2] = 5.0                     # 18:30 is the busiest quarter of the day: 5 kWh in 15 min = 20 kW
    fact = rows(spark, build_lake(tmp_path, {"TR-1": quarters}, {h: 10.0 + h for h in range(24)}))
    assert len(fact) == 24
    r = fact[("TR-1", 18)]
    assert (r.date_key, r.circuit_code, r.delivered_kwh, r.peak_kw) == (20130120, "CT-1", 8.0, 20.0)
    assert r.load_pct_capacity == 0.2              # 20 kW over 100 kVA
    assert r.temperature_c == 28.0                 # F2 of 18:00
    assert fact[("TR-1", 3)].peak_kw == 4.0        # 1 kWh per quarter = 4 kW


def test_an_hour_with_a_missing_quarter_is_dropped(spark, tmp_path):
    quarters = [1.0] * 96
    quarters[5 * 4 + 1] = None                     # 05:15 never arrived from F5
    fact = rows(spark, build_lake(tmp_path, {"TR-1": quarters}, {h: 15.0 for h in range(24)}))
    assert ("TR-1", 5) not in fact
    assert len(fact) == 23


def test_an_hour_without_weather_keeps_its_load_and_has_no_temperature(spark, tmp_path):
    temps = {h: 12.0 for h in range(24) if h != 7}
    fact = rows(spark, build_lake(tmp_path, {"TR-1": [2.0] * 96, "TR-2": [1.0] * 96}, temps))
    assert len(fact) == 48
    assert fact[("TR-1", 7)].temperature_c is None
    assert fact[("TR-1", 7)].load_pct_capacity == 0.08
    assert fact[("TR-2", 8)].temperature_c == 12.0
