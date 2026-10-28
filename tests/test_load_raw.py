from datetime import datetime, timezone

import pytest

from load_raw import manifests, plan, remote_size, upload_all


@pytest.fixture
def raw_dir(tmp_path):
    blocks = tmp_path / "halfhourly_dataset" / "halfhourly_dataset"
    blocks.mkdir(parents=True)
    for n in (0, 2, 10):
        (blocks / f"block_{n}.csv").write_text("LCLid,tstp,energy(kWh/hh)\n" + "x" * n)
    (tmp_path / "informations_households.csv").write_text("LCLid\n")
    (tmp_path / "weather_hourly_darksky.csv").write_text("time\n")
    return tmp_path


def test_plan_follows_the_lake_layout(raw_dir):
    keys = [u.key for u in plan(raw_dir, "2026-10-25")]
    assert keys == [
        "source=f1/dataset=halfhourly/ingest_date=2026-10-25/block_0.csv",
        "source=f1/dataset=halfhourly/ingest_date=2026-10-25/block_2.csv",
        "source=f1/dataset=halfhourly/ingest_date=2026-10-25/block_10.csv",
        "source=f1/dataset=households/ingest_date=2026-10-25/informations_households.csv",
        "source=f2/dataset=weather_hourly/ingest_date=2026-10-25/weather_hourly_darksky.csv",
    ]


def test_plan_keeps_the_real_file_sizes(raw_dir):
    sizes = {u.path.name: u.size for u in plan(raw_dir, "2026-10-25")}
    assert sizes["block_10.csv"] == (raw_dir / "halfhourly_dataset/halfhourly_dataset/block_10.csv").stat().st_size


def test_dev_subset_only_takes_its_blocks(raw_dir):
    names = [u.path.name for u in plan(raw_dir, "2026-10-25", dev_blocks=["block_2"])]
    assert names == ["block_2.csv", "informations_households.csv", "weather_hourly_darksky.csv"]


def test_missing_dataset_stops_the_load(raw_dir):
    (raw_dir / "weather_hourly_darksky.csv").unlink()
    with pytest.raises(FileNotFoundError, match="weather_hourly_darksky.csv"):
        plan(raw_dir, "2026-10-25")


def test_one_manifest_per_partition(raw_dir):
    uploads = plan(raw_dir, "2026-10-25")
    t = datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc)
    result = manifests(uploads, t, t, "sharon")
    assert sorted(result) == [
        "source=f1/dataset=halfhourly/ingest_date=2026-10-25/_manifest.json",
        "source=f1/dataset=households/ingest_date=2026-10-25/_manifest.json",
        "source=f2/dataset=weather_hourly/ingest_date=2026-10-25/_manifest.json",
    ]
    blocks = result["source=f1/dataset=halfhourly/ingest_date=2026-10-25/_manifest.json"]
    assert blocks["source"] == "f1" and blocks["dataset"] == "halfhourly"
    assert [f["name"] for f in blocks["files"]] == ["block_0.csv", "block_2.csv", "block_10.csv"]
    assert blocks["bytes"] == sum(u.size for u in uploads[:3])
    assert blocks["loaded_by"] == "sharon"


class NotFound(Exception):
    response = {"Error": {"Code": "404"}}


class Forbidden(Exception):
    response = {"Error": {"Code": "403"}}


class FakeS3:
    def __init__(self, existing=None, error=NotFound):
        self.existing = existing or {}
        self.error = error
        self.uploaded = []

    def head_object(self, Bucket, Key):
        if Key in self.existing:
            return {"ContentLength": self.existing[Key]}
        raise self.error()

    def upload_file(self, filename, bucket, key, **kwargs):
        self.uploaded.append(key)
        self.existing[key] = len(open(filename, "rb").read())


def test_upload_skips_files_already_loaded(raw_dir):
    uploads = plan(raw_dir, "2026-10-25")
    s3 = FakeS3(existing={uploads[0].key: uploads[0].size})
    stats = upload_all(s3, "raw", uploads, log=lambda _: None)
    assert stats["skipped"] == 1 and stats["uploaded"] == 4
    assert uploads[0].key not in s3.uploaded


def test_upload_replaces_a_partial_file(raw_dir):
    uploads = plan(raw_dir, "2026-10-25")
    s3 = FakeS3(existing={uploads[2].key: 1})
    upload_all(s3, "raw", uploads, log=lambda _: None)
    assert uploads[2].key in s3.uploaded


def test_running_twice_uploads_nothing_new(raw_dir):
    uploads = plan(raw_dir, "2026-10-25")
    s3 = FakeS3()
    upload_all(s3, "raw", uploads, log=lambda _: None)
    stats = upload_all(s3, "raw", uploads, log=lambda _: None)
    assert stats == {"uploaded": 0, "skipped": 5, "bytes": 0}


def test_access_denied_is_not_treated_as_missing():
    with pytest.raises(Forbidden):
        remote_size(FakeS3(error=Forbidden), "raw", "key")
