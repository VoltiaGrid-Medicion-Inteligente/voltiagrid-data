"""On-demand quarterly report (US-54, T-54.2): losses and metering quality of one quarter, as CSV.

Started by hand from the Airflow UI ("Trigger DAG w/ config"), not on a schedule. The quarter's daily losses must be in
the lake already (the daily DAG or the history load).

Parameters:
    quarter  the quarter to report, e.g. 2013-Q1. Required.

Output: curated/dataset=quarterly_report_csv/quarter=2013-Q1/ (CSV with a header row)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from voltiagrid_common import spark_job

default_args = {
    "owner": "voltiagrid",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="voltiagrid_quarterly_report",
    description="On demand: losses and metering quality of one quarter, as CSV (US-54)",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "quarter": Param(type="string", pattern=r"^\d{4}-Q[1-4]$", description="Quarter to report, e.g. 2013-Q1"),
    },
    tags=["voltiagrid", "report"],
) as dag:
    spark_job("quarterly_report", "--quarter {{ params.quarter }}")
