"""Time, resources and cost of a run on Fargate (US-41, T-41.2)."""

import pytest

from pipeline.costs import markdown, summarize, task_cost


def test_one_hour_of_2_vcpu_and_8_gb_on_demand():
    assert task_cost(3600, 2048, 8192) == pytest.approx(2 * 0.04048 + 8 * 0.004445)    # 0.11652 USD


def test_spot_is_cheaper_and_a_short_task_pays_one_minute():
    assert task_cost(3600, 8192, 32768, "FARGATE_SPOT") < task_cost(3600, 8192, 32768, "FARGATE")
    assert task_cost(10, 2048, 8192) == task_cost(60, 2048, 8192)


def test_worker_tasks_cost_nothing_extra_and_the_total_adds_fargate():
    s = summarize([
        {"task_id": "clean_readings", "seconds": 1800, "where": "fargate", "cpu": 8192, "memory": 32768,
         "capacity": "FARGATE_SPOT"},
        {"task_id": "export_inventory", "seconds": 30, "where": "worker", "cpu": 0, "memory": 0, "capacity": ""},
    ])
    assert s["tasks"][1]["cost"] == 0.0
    assert s["cost"] == pytest.approx(0.5 * (8 * 0.0127 + 32 * 0.0014))
    assert s["task_seconds"] == 1830


def test_report_has_the_three_sections_ca_09_asks_for():
    s = summarize([{"task_id": "clean_readings", "seconds": 1800, "where": "fargate", "cpu": 8192, "memory": 32768,
                    "capacity": "FARGATE_SPOT"}])
    text = markdown(s, "manual__2026-11-10", wall_seconds=1900)
    for section in ("## Duration", "## Resources", "## Cost"):
        assert section in text
    assert "| `clean_readings` | FARGATE_SPOT | 8 | 32 |" in text
    assert "31.7 minutes" in text
