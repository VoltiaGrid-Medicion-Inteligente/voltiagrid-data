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
