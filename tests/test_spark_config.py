"""Spark settings for the S3 lake (US-40, T-40.1)."""

from jobs.spark import s3a_options


def test_no_aws_settings_means_no_s3a_options(monkeypatch):
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION", "S3_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    assert s3a_options() == {}


def test_region_of_the_buckets_comes_from_aws_region(monkeypatch):
    monkeypatch.delenv("S3_ENDPOINT", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    assert s3a_options() == {"spark.hadoop.fs.s3a.endpoint.region": "us-east-1"}


def test_minio_endpoint_uses_path_style_and_plain_http(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    options = s3a_options()
    assert options["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert options["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert options["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"


def test_no_credentials_ever_go_into_the_spark_configuration(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    assert not any("secret" in v or "AKIA" in v or "key" in k for k, v in s3a_options().items())
