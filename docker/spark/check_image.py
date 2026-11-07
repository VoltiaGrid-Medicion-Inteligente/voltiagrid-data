"""Check of the Spark jobs image (US-40, T-40.1): Java, Spark and the S3A connector work.

    docker run --rm voltiagrid/spark-jobs:local                                   # local Spark and S3A classes
    docker run --rm -e CHECK_S3_URI=s3a://<bucket>/_check voltiagrid/spark-jobs:local   # also a write and read on S3

With CHECK_S3_URI it writes 5 rows as Parquet there and reads them back, using the same credentials as the jobs
(the ECS task role in AWS, or S3_ENDPOINT and AWS_* variables for MinIO in the CI).
"""

import os

from jobs.spark import get_spark


def main() -> None:
    spark = get_spark("voltiagrid-image-check", ui=False)
    jvm = spark.sparkContext._jvm
    print("java", jvm.java.lang.System.getProperty("java.version"), "| spark", spark.version,
          "| hadoop", jvm.org.apache.hadoop.util.VersionInfo.getVersion())
    print("s3a", jvm.java.lang.Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem").getName())
    uri = os.getenv("CHECK_S3_URI")
    if uri:
        spark.range(5).write.mode("overwrite").parquet(uri)
        print("s3 write and read:", spark.read.parquet(uri).count(), "rows at", uri)
    spark.stop()


if __name__ == "__main__":
    main()
