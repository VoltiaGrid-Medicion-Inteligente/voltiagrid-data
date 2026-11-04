"""Paths of the data lake, as in the *Data lake layout* page of voltiagrid-docs (ADR-003).

    <root>/raw/source=f3/dataset=readings/ingest_date=2026-11-02/ingest_hour=14/DC-TR-00001_00000123.json
    <root>/clean/source=readings/dataset=interval_30min/date=2013-01-15/part-....snappy.parquet
    <root>/curated/dataset=fact_consumption/month=2013-01/part-....snappy.parquet

Locally the root is a folder with one subfolder per layer (`lake/` in voltiagrid-api, where the F3 consumer
writes). In AWS each layer is its own bucket; pass --raw, --clean and --curated with the s3a:// URIs instead.
"""

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class Lake:
    raw_root: str
    clean_root: str
    curated_root: str

    @classmethod
    def local(cls, root: str) -> "Lake":
        root = root.rstrip("/")
        return cls(f"{root}/raw", f"{root}/clean", f"{root}/curated")

    def raw(self, source: str, dataset: str) -> str:
        return f"{self.raw_root}/source={source}/dataset={dataset}"

    def clean(self, source: str, dataset: str) -> str:
        return f"{self.clean_root}/source={source}/dataset={dataset}"

    def curated(self, dataset: str) -> str:
        return f"{self.curated_root}/dataset={dataset}"


def add_lake_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--lake", default="lake", help="local folder with raw/, clean/ and curated/ (default: lake)")
    parser.add_argument("--raw", default=None, help="raw layer root, e.g. s3a://voltiagrid-dev-raw-<account>")
    parser.add_argument("--clean", default=None, help="clean layer root")
    parser.add_argument("--curated", default=None, help="curated layer root")


def lake_from_args(args: argparse.Namespace) -> Lake:
    local = Lake.local(args.lake)
    return Lake(args.raw or local.raw_root, args.clean or local.clean_root, args.curated or local.curated_root)


# ---------- small files next to the data (manifests, _SUCCESS), on a local folder or S3 ----------

def put_object(path: str, content: str | bytes) -> None:
    """Write one raw object without Spark: a local file, or an S3 object when the path is s3a:// or s3://."""
    body = content.encode("utf-8") if isinstance(content, str) else content
    if path.startswith(("s3a://", "s3://")):
        import boto3

        bucket, key = path.split("://", 1)[1].split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)
        return
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)


def _filesystem(spark, path: str):
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path


def path_exists(spark, path: str) -> bool:
    fs, hadoop_path = _filesystem(spark, path)
    return fs.exists(hadoop_path)


def write_text(spark, path: str, text: str) -> None:
    fs, hadoop_path = _filesystem(spark, path)
    stream = fs.create(hadoop_path, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def read_text(spark, path: str) -> str:
    fs, hadoop_path = _filesystem(spark, path)
    stream = fs.open(hadoop_path)
    reader = spark.sparkContext._jvm.java.io.BufferedReader(
        spark.sparkContext._jvm.java.io.InputStreamReader(stream, "UTF-8"))
    try:
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        return "\n".join(lines)
    finally:
        reader.close()


# ---------- which reading dates a job processes ----------

def add_date_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", action="append", default=None, metavar="YYYY-MM-DD",
                       help="reading date to process (repeat for several)")
    group.add_argument("--run", default=None,
                       help="process the dates touched by this cleaning run (manifest in clean/.../_runs/)")


def dates_from_args(spark, args: argparse.Namespace, lake: Lake) -> list[str]:
    """Reading dates as YYYY-MM-DD strings, from --date or from the manifest of a cleaning run."""
    import json

    if args.date:
        return sorted(set(args.date))
    manifest = f"{lake.clean('readings', 'interval_30min')}/_runs/{args.run}.json"
    if not path_exists(spark, manifest):
        raise SystemExit(f"run manifest not found: {manifest}")
    return sorted(json.loads(read_text(spark, manifest))["dates"])


def add_month_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", action="append", default=None, metavar="YYYY-MM",
                       help="month to process (repeat for several)")
    group.add_argument("--run", default=None, help="process the months touched by this cleaning run")


def months_from_args(spark, args: argparse.Namespace, lake: Lake) -> list[str]:
    if args.month:
        return sorted(set(args.month))
    args.date = None
    return sorted({d[:7] for d in dates_from_args(spark, args, lake)})
