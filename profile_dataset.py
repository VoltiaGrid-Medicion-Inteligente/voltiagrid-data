import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StructType

from schemas.f1 import F1_SCHEMA, HOUSEHOLDS_SCHEMA
from schemas.f2 import F2_SCHEMA
from schemas.header_check import assert_header

ENERGY = "energy(kWh/hh)"
DEV_HOUSEHOLDS = 200


def block_number(path: Path) -> int:
    match = re.search(r"block_(\d+)\.csv$", path.name)
    return int(match.group(1)) if match else -1


def find_files(raw_dir: Path) -> dict:
    blocks = sorted(raw_dir.rglob("block_*.csv"), key=block_number)
    if not blocks:
        raise FileNotFoundError(f"No block_*.csv files found under {raw_dir.resolve()}")

    def single(name: str) -> Path:
        found = sorted(raw_dir.rglob(name))
        if not found:
            raise FileNotFoundError(f"{name} not found under {raw_dir.resolve()}")
        return found[0]

    return {
        "blocks": blocks,
        "households": single("informations_households.csv"),
        "weather": single("weather_hourly_darksky.csv"),
    }


def size_mb(paths: list[Path]) -> float:
    return round(sum(p.stat().st_size for p in paths) / 1024 / 1024, 1)


def read_csv(spark: SparkSession, paths: list[Path], schema: StructType) -> DataFrame:
    for path in paths[:1] if len(paths) > 20 else paths:
        assert_header(path, schema.fieldNames())
    return spark.read.csv([str(p) for p in paths], header=True, schema=schema)


def null_counts(df: DataFrame) -> dict:
    row = df.select([F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c) for c in df.columns]).first()
    return {c: int(row[c] or 0) for c in df.columns}


def numeric_ranges(df: DataFrame) -> dict:
    cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    if not cols:
        return {}
    aggs = []
    for c in cols:
        aggs += [F.min(F.col(f"`{c}`")).alias(f"{c}__min"), F.max(F.col(f"`{c}`")).alias(f"{c}__max")]
    row = df.select(aggs).first()
    return {c: (row[f"{c}__min"], row[f"{c}__max"]) for c in cols}


def profile_f1(spark: SparkSession, files: dict) -> tuple[dict, DataFrame]:
    df = read_csv(spark, files["blocks"], F1_SCHEMA).withColumn("block", F.col("_metadata.file_name"))
    df.cache()

    energy_text = F.col(f"`{ENERGY}`")
    energy_num = energy_text.try_cast("double")
    summary = df.select(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("LCLid").alias("households"),
        F.min("tstp").alias("first_ts"),
        F.max("tstp").alias("last_ts"),
        F.sum(energy_text.isNull().cast("int")).alias("energy_null"),
        F.sum((F.trim(energy_text) == "Null").cast("int")).alias("energy_null_text"),
        F.min(energy_num).alias("energy_min"),
        F.max(energy_num).alias("energy_max"),
        F.sum((energy_num < 0).cast("int")).alias("energy_negative"),
        F.sum((energy_text.isNotNull() & (F.trim(energy_text) != "Null") & energy_num.isNull()).cast("int"))
        .alias("energy_other_text"),
    ).first().asDict()

    per_block = df.groupBy("block").count().collect()
    counts = sorted(r["count"] for r in per_block)

    result = {
        "files": len(files["blocks"]),
        "size_mb": size_mb(files["blocks"]),
        "nulls": null_counts(df.drop("block")),
        "rows_per_block": (counts[0], round(sum(counts) / len(counts)), counts[-1]),
        **summary,
    }
    return result, df


def profile_households(spark: SparkSession, files: dict) -> tuple[dict, DataFrame]:
    df = read_csv(spark, [files["households"]], HOUSEHOLDS_SCHEMA)
    result = {
        "rows": df.count(),
        "households": df.select("LCLid").distinct().count(),
        "nulls": null_counts(df),
        "size_mb": size_mb([files["households"]]),
    }
    return result, df


def profile_f2(spark: SparkSession, files: dict) -> dict:
    df = read_csv(spark, [files["weather"]], F2_SCHEMA)
    dates = df.select(F.min("time").alias("first_ts"), F.max("time").alias("last_ts")).first()
    return {
        "rows": df.count(),
        "size_mb": size_mb([files["weather"]]),
        "first_ts": dates["first_ts"],
        "last_ts": dates["last_ts"],
        "nulls": null_counts(df),
        "ranges": numeric_ranges(df),
    }


def dev_subset(households: DataFrame, n: int) -> dict:
    """First n LCLid in alphabetical order and the blocks that contain them."""
    chosen = households.orderBy("LCLid").limit(n)
    blocks = sorted({r["file"] for r in chosen.select("file").distinct().collect()},
                    key=lambda b: block_number(Path(f"{b}.csv")))
    first_last = chosen.agg(F.min("LCLid").alias("first"), F.max("LCLid").alias("last")).first()
    return {"n": n, "blocks": blocks, "first": first_last["first"], "last": first_last["last"]}


# ---------- report ----------

def schema_table(schema: StructType) -> list[str]:
    lines = ["| Column | Spark type | Nullable |", "|---|---|---|"]
    lines += [f"| `{f.name}` | {f.dataType.simpleString()} | {f.nullable} |" for f in schema.fields]
    return lines


def nulls_table(nulls: dict, rows: int) -> list[str]:
    lines = ["| Column | Nulls | % |", "|---|---|---|"]
    for col, value in nulls.items():
        pct = 100 * value / rows if rows else 0
        lines.append(f"| `{col}` | {value:,} | {pct:.3f}% |")
    return lines


def build_report(f1: dict, hh: dict, f2: dict, dev: dict) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lo, avg, hi = f1["rows_per_block"]
    lines = [
        "# F1 and F2 data profile",
        "",
        f"Generated by `profile_dataset.py` on {generated} (US-03, KAN-77). "
        "All files are read with the explicit schemas in `schemas/`; Spark infers nothing.",
        "",
        "## Summary",
        "",
        "| Source | Files | Size (MB) | Rows | Period |",
        "|---|---|---|---|---|",
        f"| F1 half-hourly readings | {f1['files']} | {f1['size_mb']:,} | {f1['rows']:,} | "
        f"{f1['first_ts']} to {f1['last_ts']} |",
        f"| F1 households | 1 | {hh['size_mb']:,} | {hh['rows']:,} | - |",
        f"| F2 hourly weather | 1 | {f2['size_mb']:,} | {f2['rows']:,} | {f2['first_ts']} to {f2['last_ts']} |",
        "",
        "## F1 half-hourly readings (`block_*.csv`)",
        "",
        f"- Distinct households (`LCLid`): **{f1['households']:,}**",
        f"- Rows per block: min {lo:,} / average {avg:,} / max {hi:,}",
        f"- `{ENERGY}` empty: {f1['energy_null']:,}; written as the text `Null`: **{f1['energy_null_text']:,}**",
        f"- `{ENERGY}` other non-numeric values: {f1['energy_other_text']:,}",
        f"- `{ENERGY}` range (numeric values only): {f1['energy_min']} to {f1['energy_max']} kWh; "
        f"negative values: {f1['energy_negative']:,}",
        "- Energy is loaded as text because of the `Null` strings; it is cast to double in the clean layer.",
        "",
        "### Schema",
        "",
        *schema_table(F1_SCHEMA),
        "",
        "### Nulls",
        "",
        *nulls_table(f1["nulls"], f1["rows"]),
        "",
        "## F1 households (`informations_households.csv`)",
        "",
        f"- Rows: {hh['rows']:,}; distinct `LCLid`: {hh['households']:,}",
        "",
        *schema_table(HOUSEHOLDS_SCHEMA),
        "",
        *nulls_table(hh["nulls"], hh["rows"]),
        "",
        "## F2 hourly weather (`weather_hourly_darksky.csv`)",
        "",
        *schema_table(F2_SCHEMA),
        "",
        "### Nulls",
        "",
        *nulls_table(f2["nulls"], f2["rows"]),
        "",
        "### Numeric ranges",
        "",
        "| Column | Min | Max |",
        "|---|---|---|",
        *[f"| `{c}` | {lo_} | {hi_} |" for c, (lo_, hi_) in f2["ranges"].items()],
        "",
        "## Development subset",
        "",
        f"The first {dev['n']} households in alphabetical order (`{dev['first']}` to `{dev['last']}`) "
        f"are stored in these blocks: {', '.join(f'`{b}`' for b in dev['blocks'])}.",
        "",
        "T-03.3 documents why this subset is representative and how to load only these blocks.",
        "",
    ]
    return "\n".join(lines)


# ---------- main ----------

def main() -> None:
    parser = argparse.ArgumentParser(description="Profile F1 and F2")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output", type=Path, default=Path("docs/data/profile.md"))
    parser.add_argument("--dev-households", type=int, default=DEV_HOUSEHOLDS)
    parser.add_argument("--driver-memory", default="4g")
    args = parser.parse_args()

    files = find_files(args.raw_dir)
    print(f"F1 blocks found: {len(files['blocks'])}")

    spark = (
        SparkSession.builder.appName("voltiagrid-profile-f1-f2")
        .master("local[*]")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        f1, _ = profile_f1(spark, files)
        hh, households = profile_households(spark, files)
        f2 = profile_f2(spark, files)
        dev = dev_subset(households, args.dev_households)
    finally:
        spark.stop()

    report = build_report(f1, hh, f2, dev)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(report)
    print(f"Report written to {args.output}")


if __name__ == "__main__":
    main()