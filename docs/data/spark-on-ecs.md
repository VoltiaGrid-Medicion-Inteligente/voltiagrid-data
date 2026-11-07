# Spark on ECS Fargate (sprint 5)

ADR-002 and ADR-014: the Spark jobs run in **local mode inside one container** (one process with every core of the
task), on Fargate on-demand for the daily batches and on Fargate Spot for the one-off F1 history (US-41). Airflow only
starts the tasks. The lake is the three S3 buckets of `voltiagrid-api/infra/envs/data` (raw, clean, curated).

## 1. The image (US-40, T-40.1)

`docker/spark/Dockerfile` builds `voltiagrid/spark-jobs`: Python 3.12, Java 17, the versions of `requirements.txt` and
the code of `jobs/`, `pipeline/`, `schemas/`, `data/reference/` and `dev_subset.py` (the F1 dev blocks).

The **S3A connector** (`s3a://` paths) is not part of PySpark. `docker/spark/fetch_s3a_jars.py` asks Spark at build
time for `org.apache.hadoop:hadoop-aws` of the same Hadoop version that PySpark ships (3.5.0 with PySpark 4.2.0) and
copies it, with the AWS SDK version it was built with, into PySpark's `jars/` folder. No version is written by hand,
so upgrading PySpark upgrades the connector. The AWS SDK bundle is large: the image is about 1.5 GB (ECR storage
about 0.15 USD a month; Fargate pulls it in about a minute).

```powershell
docker build -f docker/spark/Dockerfile -t voltiagrid/spark-jobs:local .
docker run --rm voltiagrid/spark-jobs:local                      # java, spark, hadoop versions and the S3A class
docker run --rm voltiagrid/spark-jobs:local python -m jobs.clean_readings --help
```

**Credentials.** None in the image or in the Spark configuration. S3A's default chain finds them: on Fargate the ECS
task role (T-40.2); on a laptop `AWS_PROFILE` or the `AWS_*` variables. `jobs/spark.py` only adds the region of the
buckets (`AWS_REGION`) and, for an S3-compatible server such as MinIO, `S3_ENDPOINT` with path-style URLs.

**CI.** `.github/workflows/spark-image.yml` builds the image when `docker/spark/`, the jobs or `requirements.txt`
change, starts a MinIO server, and runs `docker/spark/check_image.py`: Spark writes 5 rows as Parquet to
`s3a://voltiagrid-ci/_check` and reads them back. That is the same code path as the lake buckets, without AWS
credentials in the CI.

The container runs as user `spark` (uid 10001), not root. The jobs only need `/tmp` (Spark's scratch space).
