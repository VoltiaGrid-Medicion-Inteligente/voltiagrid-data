"""Build step of docker/spark/Dockerfile: put the S3A connector in PySpark's jars folder (US-40, T-40.1).

hadoop-aws must be the same version as the Hadoop client inside PySpark, and it needs the AWS SDK version it was built
with. Asking Spark to resolve `org.apache.hadoop:hadoop-aws:<version>` (Ivy, from Maven Central) brings exactly that
pair, so no version is written by hand here.
"""

import shutil
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

JARS = Path(pyspark.__file__).parent / "jars"


def hadoop_version() -> str:
    jar = next(JARS.glob("hadoop-client-api-*.jar"))
    return jar.stem.removeprefix("hadoop-client-api-")


def main() -> None:
    version = hadoop_version()
    spark = (SparkSession.builder.master("local[1]").appName("fetch-s3a")
             .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{version}")
             .config("spark.jars.ivy", "/tmp/ivy")
             .getOrCreate())
    spark.stop()
    present = {p.name for p in JARS.glob("*.jar")}
    added = []
    for jar in sorted(Path("/tmp/ivy/jars").glob("*.jar")):
        if jar.name not in present:
            shutil.copy(jar, JARS / jar.name)
            added.append(jar.name)
    if not any("_hadoop-aws-" in name for name in added):
        raise SystemExit("hadoop-aws was not resolved")
    print(f"Hadoop {version}: added {', '.join(added)}")


if __name__ == "__main__":
    main()
