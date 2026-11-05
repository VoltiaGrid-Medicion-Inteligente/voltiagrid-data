"""Copy one day of F5 (energy delivered to each transformer every 15 minutes) to raw (US-37, T-37.1).

    python -m jobs.fetch_f5 --date 2013-01-05 --lake ../voltiagrid-api/lake       # F5_URL, default localhost:8005
    python -m jobs.fetch_f5 --run f3-2026-11-02 --lake ../voltiagrid-api/lake     # the dates of a cleaning run

One object per transformer and day, the API answer byte for byte (Data lake layout):
    raw/source=f5/dataset=transformer_energy/ingest_date=<today>/<transformer>_<from>_<to>.json
    e.g. TR-00001_20130105T0000Z_20130106T0000Z.json
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from jobs.lake import Lake, add_date_arguments, add_lake_arguments, lake_from_args, put_object, read_run_dates


def http_get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def object_name(transformer: str, day: date) -> str:
    start, end = day, day + timedelta(days=1)
    return f"{transformer}_{start:%Y%m%d}T0000Z_{end:%Y%m%d}T0000Z.json"


def fetch_day(lake: Lake, base_url: str, day: date, ingest_date: date,
              get: Callable[[str], bytes] = http_get) -> list[str]:
    transformers = [t["transformer_code"] for t in json.loads(get(f"{base_url}/transformers"))]
    query = urllib.parse.urlencode({"start": f"{day.isoformat()}T00:00:00Z",
                                    "end": f"{(day + timedelta(days=1)).isoformat()}T00:00:00Z"})
    prefix = f"{lake.raw('f5', 'transformer_energy')}/ingest_date={ingest_date.isoformat()}"
    written = []
    for code in transformers:
        body = get(f"{base_url}/transformers/{urllib.parse.quote(code)}/energy?{query}")
        path = f"{prefix}/{object_name(code, day)}"
        put_object(path, body)
        written.append(path)
    return written


def main() -> None:
    p = argparse.ArgumentParser(description="Copy F5 transformer energy to raw (US-37)")
    add_lake_arguments(p)
    add_date_arguments(p)
    p.add_argument("--f5-url", default=os.getenv("F5_URL", "http://localhost:8005"))
    args = p.parse_args()
    lake = lake_from_args(args)
    days = read_run_dates(lake, args.run) if args.run else sorted(set(args.date))
    today = datetime.now(timezone.utc).date()
    for d in days:
        written = fetch_day(lake, args.f5_url.rstrip("/"), date.fromisoformat(d), today)
        print(f"fetch_f5: {d}: {len(written)} transformers -> {written[0].rsplit('/', 1)[0] if written else '-'}")


if __name__ == "__main__":
    main()
