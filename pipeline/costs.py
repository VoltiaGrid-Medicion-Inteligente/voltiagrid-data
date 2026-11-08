"""Time, resources and cost of a pipeline run on Fargate (US-40 CA "cost per run", US-41 T-41.2, CA-09).

Plain Python (no Spark, no Airflow) so the DAGs and the tests import it. Fargate bills per second each vCPU and GB
of the task while it runs. Prices: Linux/x86 in us-east-1 (https://aws.amazon.com/fargate/pricing/, checked
2026-11). Spot changes with demand: check the page before reporting a cost and update SPOT here if it moved.
"""

PRICES = {                          # USD per vCPU-hour, USD per GB-hour
    "FARGATE": (0.04048, 0.004445),
    "FARGATE_SPOT": (0.0127, 0.0014),
}
MIN_SECONDS = 60                    # Fargate bills one minute at least


def task_cost(seconds: float, cpu_units: int, memory_mib: int, capacity: str = "FARGATE") -> float:
    """Cost of one Fargate task: billed seconds x (vCPU x price + GB x price). 1024 CPU units = 1 vCPU."""
    vcpu_price, gb_price = PRICES[capacity]
    hours = max(seconds, MIN_SECONDS) / 3600
    return hours * (cpu_units / 1024 * vcpu_price + memory_mib / 1024 * gb_price)


def summarize(tasks: list[dict]) -> dict:
    """tasks: {task_id, seconds, where ('fargate' or 'worker'), cpu, memory, capacity}. Cost only for Fargate tasks;
    the worker (the Airflow instance) is paid by the hour whatever runs on it."""
    rows = []
    for t in tasks:
        cost = task_cost(t["seconds"], t["cpu"], t["memory"], t["capacity"]) if t["where"] == "fargate" else 0.0
        rows.append(t | {"cost": cost})
    seconds = [t["seconds"] for t in rows]
    return {"tasks": rows, "task_seconds": sum(seconds), "cost": sum(t["cost"] for t in rows)}


def markdown(summary: dict, run_id: str, wall_seconds: float) -> str:
    """The Duration, Resources and Cost sections of docs/data/cloud-run.md, ready to paste."""
    lines = [f"Run `{run_id}`.", "", "## Duration", "",
             f"Wall clock of the run: **{wall_seconds / 60:.1f} minutes** "
             f"(sum of the tasks: {summary['task_seconds'] / 60:.1f} minutes; tasks without a dependency run in parallel).",
             "", "| Task | Minutes |", "|---|---|"]
    lines += [f"| `{t['task_id']}` | {t['seconds'] / 60:.1f} |" for t in summary["tasks"]]
    lines += ["", "## Resources", "", "| Task | Runs on | vCPU | Memory (GB) |", "|---|---|---|---|"]
    for t in summary["tasks"]:
        if t["where"] == "fargate":
            lines.append(f"| `{t['task_id']}` | {t['capacity']} | {t['cpu'] / 1024:g} | {t['memory'] / 1024:g} |")
        else:
            lines.append(f"| `{t['task_id']}` | Airflow worker | — | — |")
    lines += ["", "## Cost", "", "| Task | USD |", "|---|---|"]
    lines += [f"| `{t['task_id']}` | {t['cost']:.4f} |" for t in summary["tasks"] if t["where"] == "fargate"]
    lines += [f"| **Total Fargate** | **{summary['cost']:.4f}** |", "",
              "Prices: " + ", ".join(f"{k} {v[0]} USD per vCPU-hour and {v[1]} per GB-hour" for k, v in PRICES.items())
              + " (us-east-1, Linux/x86)."]
    return "\n".join(lines)
