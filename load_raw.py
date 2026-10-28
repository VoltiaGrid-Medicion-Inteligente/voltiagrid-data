import argparse
import getpass
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dev_subset import block_number, load_dev_blocks, select_files

RAW_DIR = Path("data/raw")
MB = 1024 * 1024

# (file name pattern, source, dataset)
DATASETS = [
    ("block_*.csv", "f1", "halfhourly"),
    ("informations_households.csv", "f1", "households"),
    ("weather_hourly_darksky.csv", "f2", "weather_hourly"),
]


@dataclass(frozen=True)
class Upload:
    path: Path
    key: str
    size: int


def partition(source: str, dataset: str, ingest_date: str) -> str:
    return f"source={source}/dataset={dataset}/ingest_date={ingest_date}"


def plan(raw_dir: Path, ingest_date: str, dev_blocks: list[str] | None = None) -> list[Upload]:
    """Every file to upload with its key. Raises if a dataset is missing."""
    uploads = []
    for pattern, source, dataset in DATASETS:
        found = sorted(raw_dir.rglob(pattern), key=lambda p: (block_number(p.name), p.name))
        if not found:
            raise FileNotFoundError(f"{pattern} not found under {raw_dir.resolve()}")
        if pattern.startswith("block_"):
            if dev_blocks is not None:
                found = select_files(found, dev_blocks)
        else:
            found = found[:1]
        prefix = partition(source, dataset, ingest_date)
        uploads += [Upload(p, f"{prefix}/{p.name}", p.stat().st_size) for p in found]
    return uploads


def manifests(uploads: list[Upload], started: datetime, finished: datetime, loaded_by: str) -> dict[str, dict]:
    """One manifest per partition, keyed by the manifest object key."""
    out: dict[str, dict] = {}
    for u in uploads:
        prefix = u.key.rsplit("/", 1)[0]
        source, dataset = (part.split("=", 1)[1] for part in prefix.split("/")[:2])
        m = out.setdefault(f"{prefix}/_manifest.json", {
            "source": source,
            "dataset": dataset,
            "files": [],
            "bytes": 0,
            "started_at": started.isoformat(timespec="seconds"),
            "finished_at": finished.isoformat(timespec="seconds"),
            "loaded_by": loaded_by,
            "tool": "voltiagrid-data/load_raw.py",
        })
        m["files"].append({"name": u.path.name, "bytes": u.size})
        m["bytes"] += u.size
    return out


def remote_size(client, bucket: str, key: str) -> int | None:
    """Size of the object in S3, or None if it does not exist."""
    try:
        return client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except Exception as exc:  # botocore ClientError; other errors must stop the load
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def upload_all(client, bucket: str, uploads: list[Upload], config=None, log=print) -> dict:
    """Uploads every file not already in the bucket with the same size. Returns counters."""
    stats = {"uploaded": 0, "skipped": 0, "bytes": 0}
    for i, u in enumerate(uploads, start=1):
        tag = f"[{i}/{len(uploads)}] {u.path.name} ({u.size / MB:,.1f} MB)"
        if remote_size(client, bucket, u.key) == u.size:
            stats["skipped"] += 1
            log(f"{tag} already in the bucket, skipped")
            continue
        start = time.monotonic()
        extra = {"Config": config} if config is not None else {}
        client.upload_file(str(u.path), bucket, u.key, **extra)
        stats["uploaded"] += 1
        stats["bytes"] += u.size
        log(f"{tag} uploaded in {time.monotonic() - start:,.1f} s")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Load F1 and F2 into the raw layer of the data lake")
    parser.add_argument("--bucket", required=True, help="raw bucket (terraform output raw_bucket)")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--ingest-date", default=datetime.now(timezone.utc).date().isoformat(),
                        help="partition date, default today in UTC; reuse it to resume a load")
    parser.add_argument("--dev", action="store_true", help="only the blocks of the dev subset")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and upload nothing")
    args = parser.parse_args()

    uploads = plan(args.raw_dir, args.ingest_date, load_dev_blocks() if args.dev else None)
    total = sum(u.size for u in uploads)
    print(f"{len(uploads)} files, {total / MB:,.1f} MB -> s3://{args.bucket}/")
    if args.dry_run:
        for u in uploads:
            print(f"  {u.key}  ({u.size / MB:,.1f} MB)")
        return

    import boto3  # imported here so --dry-run and the tests do not need AWS
    from boto3.s3.transfer import TransferConfig

    client = boto3.client("s3")
    config = TransferConfig(multipart_threshold=64 * MB, multipart_chunksize=64 * MB, max_concurrency=8)

    started = datetime.now(timezone.utc)
    clock = time.monotonic()
    stats = upload_all(client, args.bucket, uploads, config)
    elapsed = time.monotonic() - clock
    finished = datetime.now(timezone.utc)

    for key, body in manifests(uploads, started, finished, getpass.getuser()).items():
        client.put_object(Bucket=args.bucket, Key=key, Body=json.dumps(body, indent=2).encode("utf-8"),
                          ContentType="application/json")

    speed = stats["bytes"] / MB / elapsed if elapsed > 0 else 0
    print(f"Done: {stats['uploaded']} uploaded, {stats['skipped']} skipped, "
          f"{stats['bytes'] / MB:,.1f} MB in {elapsed / 60:,.1f} min ({speed:,.1f} MB/s)")


if __name__ == "__main__":
    main()