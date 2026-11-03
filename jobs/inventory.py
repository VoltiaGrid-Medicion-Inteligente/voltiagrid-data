"""F4 network inventory for the Spark jobs (US-33, T-33.2).

The inventory lives in the operational database of voltiagrid-api (Postgres, RDS in AWS). The jobs do not query it
row by row: `export` copies it once to raw as two CSV files, and every job reads them with an explicit schema.

    python -m jobs.inventory export --lake ../voltiagrid-api/lake       # DATABASE_URL from the environment or .env

    raw/source=f4/dataset=meters/ingest_date=YYYY-MM-DD/meters.csv        meter -> customer, transformer, circuit, capacities
    raw/source=f4/dataset=contracts/ingest_date=YYYY-MM-DD/contracts.csv  contracts per customer (RN-07)

Readers take the latest ingest_date unless one is given.
"""

import argparse
import csv
import io
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.lake import Lake, add_lake_arguments, lake_from_args
from schemas.f4 import F4_CONTRACTS_SCHEMA, F4_METERS_SCHEMA

METERS_SQL = """
SELECT m.lclid AS meter_id, cu.code AS customer_code,
       t.code AS transformer_code, t.capacity_kva AS transformer_capacity_kva,
       ci.code AS circuit_code, ci.capacity_kva AS circuit_capacity_kva
FROM meter m
JOIN customer cu ON cu.id = m.customer_id
JOIN transformer t ON t.id = m.transformer_id
JOIN circuit ci ON ci.id = t.circuit_id
ORDER BY m.lclid
"""

CONTRACTS_SQL = """
SELECT cu.code AS customer_code, c.code AS contract_code, c.tariff_type, c.start_date, c.end_date
FROM contract c JOIN customer cu ON cu.id = c.customer_id
ORDER BY cu.code, c.start_date
"""


def to_csv(columns: list[str], rows: list[tuple]) -> str:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    return out.getvalue()


def psycopg_url(database_url: str) -> str:
    """The API uses SQLAlchemy URLs (postgresql+psycopg://...); psycopg wants postgresql://..."""
    return database_url.replace("postgresql+psycopg://", "postgresql://", 1)


def fetch(database_url: str) -> dict[str, tuple[list[str], list[tuple]]]:
    import psycopg

    result = {}
    with psycopg.connect(psycopg_url(database_url)) as conn, conn.cursor() as cur:
        for name, sql in (("meters", METERS_SQL), ("contracts", CONTRACTS_SQL)):
            cur.execute(sql)
            result[name] = ([d.name for d in cur.description], cur.fetchall())
    return result


def write_raw(lake: Lake, tables: dict, ingest_date: date) -> list[str]:
    """Write each table to its raw partition with a manifest. Local folder, or S3 when the raw root is s3a://."""
    written = []
    for name, (columns, rows) in tables.items():
        prefix = f"{lake.raw('f4', name)}/ingest_date={ingest_date.isoformat()}"
        body = to_csv(columns, rows)
        manifest = json.dumps({"source": "f4", "dataset": name, "files": [f"{name}.csv"], "rows": len(rows),
                               "bytes": len(body.encode()), "loaded_by": os.getenv("USERNAME") or os.getenv("USER"),
                               "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}, indent=2)
        for file_name, content in ((f"{name}.csv", body), ("_manifest.json", manifest)):
            _put(f"{prefix}/{file_name}", content)
        written.append(f"{prefix}/{name}.csv ({len(rows)} rows)")
    return written


def _put(path: str, content: str) -> None:
    if path.startswith(("s3a://", "s3://")):
        import boto3

        bucket, key = path.split("://", 1)[1].split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content.encode())
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8", newline="")


# ---------- readers ----------

def _latest_partition(spark: SparkSession, base: str) -> str:
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(base)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        raise FileNotFoundError(f"{base} does not exist: run `python -m jobs.inventory export` first")
    dates = sorted(s.getPath().getName() for s in fs.listStatus(path) if s.getPath().getName().startswith("ingest_date="))
    if not dates:
        raise FileNotFoundError(f"no ingest_date= partition under {base}")
    return f"{base}/{dates[-1]}"


def read_meters(spark: SparkSession, lake: Lake, ingest_date: str | None = None) -> DataFrame:
    base = lake.raw("f4", "meters")
    folder = f"{base}/ingest_date={ingest_date}" if ingest_date else _latest_partition(spark, base)
    return spark.read.csv(f"{folder}/meters.csv", header=True, schema=F4_METERS_SCHEMA, mode="FAILFAST")


def read_contracts(spark: SparkSession, lake: Lake, ingest_date: str | None = None) -> DataFrame:
    base = lake.raw("f4", "contracts")
    folder = f"{base}/ingest_date={ingest_date}" if ingest_date else _latest_partition(spark, base)
    return spark.read.csv(f"{folder}/contracts.csv", header=True, schema=F4_CONTRACTS_SCHEMA, mode="FAILFAST")


def circuit_capacity(meters: DataFrame) -> DataFrame:
    """One row per circuit with its capacity (kVA). Fails if F4 gives one circuit two capacities."""
    capacities = meters.groupBy("circuit_code").agg(
        F.min("circuit_capacity_kva").alias("capacity_kva"),
        F.countDistinct("circuit_capacity_kva").alias("_values"),
        F.countDistinct("transformer_code").alias("transformers"),
        F.count("*").alias("meters"),
    )
    bad = capacities.filter(F.col("_values") > 1).select("circuit_code").collect()
    if bad:
        raise ValueError(f"circuits with more than one capacity in F4: {[r.circuit_code for r in bad]}")
    return capacities.drop("_values")


def main() -> None:
    p = argparse.ArgumentParser(description="F4 inventory: export from the API database to raw")
    p.add_argument("command", choices=["export"])
    add_lake_arguments(p)
    p.add_argument("--database-url", default=None, help="default: DATABASE_URL (environment or .env)")
    p.add_argument("--ingest-date", default=None, help="default: today (UTC)")
    args = p.parse_args()
    url = args.database_url or os.getenv("DATABASE_URL") or _from_dotenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is missing: the same value as in voltiagrid-api/.env (localhost:5433)")
    ingest = date.fromisoformat(args.ingest_date) if args.ingest_date else datetime.now(timezone.utc).date()
    for line in write_raw(lake_from_args(args), fetch(url), ingest):
        print("wrote", line)


def _from_dotenv(name: str) -> str | None:
    env = Path(".env")
    if not env.exists():
        return None
    for line in env.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    return None


if __name__ == "__main__":
    main()
