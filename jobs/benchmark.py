"""Measure the daily close job by job: time, shuffle and execution plans (US-42, T-42.1).

    python -m jobs.benchmark --run f3-2026-11-04 --label before --lake ../voltiagrid-api/lake
    python -m jobs.benchmark --run f3-2026-11-04 --label after --lake ../voltiagrid-api/lake
    python -m jobs.benchmark --compare benchmark/before.json benchmark/after.json

Runs, in one Spark session and in the DAG's order, the jobs of the daily close for the dates of a cleaning run:
clean (the run's raw partition again), estimate_gaps, circuit_load, fact_consumption, billing and, when F5 is in raw,
transformer_losses and suspicious_meters. Every job overwrites only its own partitions, so measuring does not change
the result (CT-08). For each job it records, from Spark's own monitoring API (the web UI, http://localhost:4040):

    seconds        wall clock of the job
    shuffle_read   bytes read from other partitions (the expensive part: disk and network)
    shuffle_write  bytes written for a shuffle
    stages, tasks  how much work Spark split it into
    joins          joins of each type in the executed plans (BroadcastHashJoin, SortMergeJoin, ...)

Writes benchmark/<label>.json and benchmark/<label>-plans/<job>.txt (the physical plan of every query the job ran:
what US-42 asks to include). Run it twice on the same machine and data, before and after a change.
"""

import argparse
import json
import re
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from jobs.lake import Lake, add_lake_arguments, dates_from_args, lake_from_args, path_exists, read_text
from jobs.spark import get_spark

JOIN = re.compile(r"\b(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|BroadcastNestedLoopJoin|CartesianProduct)\b")


def api(spark: SparkSession, path: str):
    base = spark.sparkContext.uiWebUrl
    if not base:
        raise SystemExit("the Spark UI is off: the benchmark needs its monitoring API (spark.ui.enabled=true)")
    with urllib.request.urlopen(f"{base}/api/v1/applications/{spark.sparkContext.applicationId}/{path}") as r:
        return json.loads(r.read())


def _epoch(text: str) -> float:
    return datetime.strptime(text.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()


def measure(spark: SparkSession, name: str, step) -> dict:
    """Run one job and collect what Spark recorded between its start and end."""
    started = time.time()
    step()
    ended = time.time()
    stages = [s for s in api(spark, "stages?status=complete")
              if s.get("submissionTime") and started - 1 <= _epoch(s["submissionTime"]) <= ended + 1]
    queries = [q for q in api(spark, "sql?details=true&planDescription=true&length=100000")
               if started - 1 <= _epoch(q["submissionTime"]) <= ended + 1]
    joins = Counter(m for q in queries for m in JOIN.findall(q.get("planDescription", "")))
    return {
        "job": name,
        "seconds": round(ended - started, 1),
        "shuffle_read": sum(s.get("shuffleReadBytes", 0) for s in stages),
        "shuffle_write": sum(s.get("shuffleWriteBytes", 0) for s in stages),
        "stages": len(stages),
        "tasks": sum(s.get("numTasks", 0) for s in stages),
        "joins": dict(joins),
        "plans": [f"-- {q['description']}\n{q.get('planDescription', '')}" for q in queries],
    }


def steps(spark: SparkSession, lake: Lake, run: str) -> list[tuple]:
    """The daily close of one cleaning run, as the DAG runs it."""
    from jobs import billing, circuit_load, estimate_gaps, fact_consumption, suspicious_meters, transformer_losses
    from jobs.clean_readings import clean_path, f1_paths, merge_and_write
    from jobs.readings import read_f1, read_f3

    manifest = json.loads(read_text(spark, f"{clean_path(lake)}/_runs/{run}.json"))
    days = manifest["dates"]
    months = sorted({d[:7] for d in days})

    # A run made by Airflow in Docker saved its input as /lake/raw/...: point it at this lake's raw layer.
    source = manifest["input"]
    if "/raw/" in source and not source.startswith(lake.raw_root):
        source = f"{lake.raw_root}/{source.split('/raw/', 1)[1]}"

    def clean():
        raw = (read_f3(spark, [source]) if manifest["source"] == "f3"
               else read_f1(spark, f1_paths(source, run.split("-")[1])))
        merge_and_write(spark, raw, clean_path(lake))

    result = [
        ("clean_readings", clean),
        ("estimate_gaps", lambda: estimate_gaps.run(spark, lake, days)),
        ("circuit_load", lambda: circuit_load.run(spark, lake, days)),
        ("fact_consumption", lambda: fact_consumption.run(spark, lake, months)),
        ("billing", lambda: billing.run(spark, lake, months)),
    ]
    if path_exists(spark, lake.raw("f5", "transformer_energy")):
        result += [("transformer_losses", lambda: transformer_losses.run(spark, lake, days)),
                   ("suspicious_meters", lambda: suspicious_meters.run(spark, lake, days))]
    return result


def table(results: list[dict]) -> str:
    lines = ["| Job | Seconds | Shuffle read (MB) | Shuffle write (MB) | Stages | Tasks | Joins |", "|---|---|---|---|---|---|---|"]
    for r in results:
        joins = ", ".join(f"{k} {v}" for k, v in sorted(r["joins"].items())) or "—"
        lines.append(f"| `{r['job']}` | {r['seconds']} | {r['shuffle_read'] / 1e6:.1f} | "
                     f"{r['shuffle_write'] / 1e6:.1f} | {r['stages']} | {r['tasks']} | {joins} |")
    total = sum(r["seconds"] for r in results)
    lines.append(f"| **Total** | **{total:.1f}** | {sum(r['shuffle_read'] for r in results) / 1e6:.1f} | "
                 f"{sum(r['shuffle_write'] for r in results) / 1e6:.1f} | | | |")
    return "\n".join(lines)


def compare(before: dict, after: dict) -> str:
    lines = ["| Job | Seconds before → after | Shuffle (MB) before → after |", "|---|---|---|"]
    by_job = {r["job"]: r for r in after["results"]}
    for b in before["results"]:
        a = by_job.get(b["job"])
        if a:
            lines.append(f"| `{b['job']}` | {b['seconds']} → {a['seconds']} | "
                         f"{(b['shuffle_read'] + b['shuffle_write']) / 1e6:.1f} → "
                         f"{(a['shuffle_read'] + a['shuffle_write']) / 1e6:.1f} |")
    tb, ta = (sum(r["seconds"] for r in x["results"]) for x in (before, after))
    lines.append(f"| **Total** | **{tb:.1f} → {ta:.1f} ({ta / tb - 1:+.0%})** | |")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description="Measure the daily close job by job (US-42)")
    add_lake_arguments(p)
    p.add_argument("--run", help="cleaning run to measure, e.g. f3-2026-11-04")
    p.add_argument("--label", default="before", help="name of the measurement: before, after, ...")
    p.add_argument("--out", default="benchmark", help="folder for <label>.json and <label>-plans/ (git-ignored)")
    p.add_argument("--compare", nargs=2, metavar=("BEFORE_JSON", "AFTER_JSON"))
    args = p.parse_args()
    if args.compare:
        before, after = (json.loads(Path(f).read_text(encoding="utf-8")) for f in args.compare)
        print(compare(before, after))
        return
    if not args.run:
        raise SystemExit("give --run (or --compare)")
    args.date = None
    lake = lake_from_args(args)
    spark = get_spark("voltiagrid-benchmark", ui=True)
    dates = dates_from_args(spark, args, lake)
    results = [measure(spark, name, step) for name, step in steps(spark, lake, args.run)]
    out = Path(args.out)
    plans = out / f"{args.label}-plans"
    plans.mkdir(parents=True, exist_ok=True)
    for r in results:
        (plans / f"{r['job']}.txt").write_text("\n\n".join(r.pop("plans")), encoding="utf-8")
    report = {"label": args.label, "run": args.run, "dates": len(dates), "spark": spark.version,
              "cores": spark.sparkContext.defaultParallelism,
              "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
              "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "results": results}
    (out / f"{args.label}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"benchmark {args.label}: run {args.run}, {len(dates)} dates, {report['cores']} cores, "
          f"shuffle partitions {report['shuffle_partitions']}")
    print(table(results))
    print(f"wrote {out / (args.label + '.json')} and {plans}/")
    spark.stop()


if __name__ == "__main__":
    main()
