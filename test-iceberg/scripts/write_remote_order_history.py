"""Write a fresh batch into the remote Glue-backed Iceberg table.

This helper keeps the write path simple: generate a handful of synthetic rows
for the `order_history` schema and overwrite the `2024-07-01` partition inside
the Glue catalog that points at the remote warehouse.
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RUNTIME_JAR = (
    REPO_ROOT / "vendor" / "iceberg-spark-runtime-3.5_2.12-1.5.0.jar"
)
DEFAULT_AWS_BUNDLE_JAR = REPO_ROOT / "vendor" / "iceberg-aws-bundle-1.5.0.jar"


def resolve_runtime_jar(override: Optional[Path] = None) -> Path:
    if override is not None:
        jar_path = override.expanduser().resolve()
        if not jar_path.is_file():
            raise FileNotFoundError(f"Provided runtime JAR not found: {jar_path}")
        return jar_path

    env_override = os.environ.get("ICEBERG_SPARK_RUNTIME_JAR")
    if env_override:
        jar_path = Path(env_override).expanduser().resolve()
        if not jar_path.is_file():
            raise FileNotFoundError(
                f"ICEBERG_SPARK_RUNTIME_JAR points to missing file: {jar_path}"
            )
        return jar_path

    if DEFAULT_RUNTIME_JAR.is_file():
        return DEFAULT_RUNTIME_JAR

    raise FileNotFoundError(
        "Iceberg runtime JAR not found. Set ICEBERG_SPARK_RUNTIME_JAR "
        "or place the JAR under vendor/."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write dummy partition data via Glue catalog.")
    parser.add_argument(
        "--catalog",
        default="glue",
        help="Iceberg catalog name configured inside Spark (default: glue).",
    )
    parser.add_argument(
        "--warehouse",
        default="s3://iceberg-test-database/iceberg_db.db",
        help="S3 warehouse root backing the Glue catalog.",
    )
    parser.add_argument(
        "--database",
        default="iceberg_demo",
        help="Glue database containing the Iceberg table.",
    )
    parser.add_argument(
        "--table",
        default="order_history",
        help="Glue table to append or overwrite.",
    )
    parser.add_argument(
        "--order-date",
        default="2024-07-01",
        help="Partition date to target in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--ingest-batch-id",
        type=int,
        default=5,
        help="Batch identifier stored on each row.",
    )
    parser.add_argument(
        "--runtime-jar",
        type=Path,
        default=None,
        help="Optional override for the Iceberg runtime JAR.",
    )
    parser.add_argument(
        "--aws-bundle-jar",
        type=Path,
        default=None,
        help="Optional override for the Iceberg AWS bundle JAR.",
    )
    parser.add_argument(
        "--region",
        default="ap-northeast-2",
        help="AWS region for the Glue/S3 clients.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Print the rows created for manual inspection before writing.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("overwrite", "append"),
        default="overwrite",
        help="Use Iceberg overwritePartitions (default) or append into the table.",
    )
    return parser.parse_args()


def resolve_aws_bundle(override: Optional[Path] = None) -> Path:
    if override is not None:
        jar_path = override.expanduser().resolve()
        if not jar_path.is_file():
            raise FileNotFoundError(f"Provided AWS bundle JAR not found: {jar_path}")
        return jar_path

    if DEFAULT_AWS_BUNDLE_JAR.is_file():
        return DEFAULT_AWS_BUNDLE_JAR

    raise FileNotFoundError(
        "Iceberg AWS bundle JAR not found. Specify --aws-bundle-jar or place the file under vendor/."
    )


def build_spark_session(
    catalog: str,
    warehouse: str,
    region: str,
    jars: List[Path],
) -> SparkSession:
    builder = SparkSession.builder.appName("order-history-glue-writer")
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    builder = builder.config(
        f"spark.sql.catalog.{catalog}",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    builder = builder.config(
        f"spark.sql.catalog.{catalog}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    builder = builder.config(
        f"spark.sql.catalog.{catalog}.warehouse",
        warehouse,
    )
    builder = builder.config(
        f"spark.sql.catalog.{catalog}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    builder = builder.config(
        f"spark.sql.catalog.{catalog}.client.region",
        region,
    )
    jar_paths = ",".join(str(jar) for jar in jars)
    builder = builder.config("spark.jars", jar_paths)
    builder = builder.config("spark.sql.shuffle.partitions", "1")
    return builder.getOrCreate()


def generate_rows(target_date: date, ingest_batch_id: int, ingest_ts: datetime) -> List[dict]:
    templates = [
        ("ORD-5001", "C-24", "DELIVERED", 142.35),
        ("ORD-5002", "C-25", "PROCESSING", 88.20),
        ("ORD-5003", "C-26", "CREATED", 310.00),
        ("ORD-5004", "C-27", "ON_HOLD", 54.90),
        ("ORD-5005", "C-28", "CREATED", 78.10),
    ]

    rows: List[dict] = []
    for order_id, customer_id, status, total_amount in templates:
        rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": target_date,
                "status": status,
                "total_amount": float(total_amount),
                "updated_at": ingest_ts,
                "ingest_batch_id": ingest_batch_id,
            }
        )
    return rows


def preview_rows(rows: Iterable[dict]) -> None:
    print("Previewing rows:")
    for row in rows:
        print(row)


def main() -> None:
    args = parse_args()
    target_date = date.fromisoformat(args.order_date)
    runtime_jar = resolve_runtime_jar(args.runtime_jar)
    aws_bundle = resolve_aws_bundle(args.aws_bundle_jar)
    ingest_ts = datetime.utcnow()

    spark = build_spark_session(
        catalog=args.catalog,
        warehouse=args.warehouse,
        region=args.region,
        jars=[runtime_jar, aws_bundle],
    )

    rows = generate_rows(target_date, args.ingest_batch_id, ingest_ts)
    if args.preview:
        preview_rows(rows)

    df = spark.createDataFrame(rows)
    df = df.withColumn("order_date", F.col("order_date").cast("date"))
    df = df.withColumn("updated_at", F.col("updated_at").cast("timestamp"))

    full_table_name = f"{args.catalog}.{args.database}.{args.table}"
    writer = df.writeTo(full_table_name)
    if args.write_mode == "overwrite":
        writer.overwritePartitions()
    else:
        writer.append()

    spark.read.table(full_table_name).filter(F.col("order_date") == target_date).show(truncate=False)


if __name__ == "__main__":
    main()
