"""Simulate near-real-time ingestion into an Apache Iceberg table.

This script creates an Iceberg table partitioned by order_date, then mimics three
micro-batches of incoming data. Each batch overwrites the affected partitions,
allowing us to validate how Iceberg handles overlapping data written in overwrite mode.

Usage (requires Spark with Iceberg runtime on the classpath)::

    spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
      scripts/run_iceberg_overwrite_demo.py

The job writes to a local warehouse directory inside this repository (./warehouse).
Feel free to remove that directory between runs.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Default warehouse directory lives inside the repository unless overridden.
DEFAULT_WAREHOUSE_DIR = Path(__file__).resolve().parent.parent / "warehouse"
DEFAULT_RUNTIME_JAR = (
    Path(__file__).resolve().parent.parent
    / "vendor"
    / "iceberg-spark-runtime-3.5_2.12-1.5.0.jar"
)
CATALOG_NAME = "iceberg"
TABLE_NAME = f"{CATALOG_NAME}.order_history"


def resolve_warehouse_dir(override: Path | None = None) -> Path:
    """Resolve the warehouse directory from an explicit override or env var."""
    if override is not None:
        return override

    env_override = os.environ.get("ICEBERG_WAREHOUSE_DIR")
    if env_override:
        return Path(env_override).expanduser().resolve()

    return DEFAULT_WAREHOUSE_DIR


def resolve_runtime_jar(override: Path | None = None) -> Path:
    """Resolve the Iceberg Spark runtime JAR from an override or env var."""
    if override is not None:
        return override

    env_override = os.environ.get("ICEBERG_SPARK_RUNTIME_JAR")
    if env_override:
        jar_path = Path(env_override).expanduser().resolve()
        if not jar_path.is_file():
            raise FileNotFoundError(f"ICEBERG_SPARK_RUNTIME_JAR points to missing file: {jar_path}")
        return jar_path

    if DEFAULT_RUNTIME_JAR.is_file():
        return DEFAULT_RUNTIME_JAR

    raise FileNotFoundError(
        "Iceberg runtime JAR not found. Set ICEBERG_SPARK_RUNTIME_JAR or place the JAR under vendor/."
    )


def build_spark_session(warehouse_dir: Path, runtime_jar: Path) -> SparkSession:
    """Configure Spark with the Iceberg catalog backed by the local filesystem."""
    warehouse_path = str(warehouse_dir)
    # Spark will create the warehouse path if it does not already exist.
    os.makedirs(warehouse_path, exist_ok=True)

    return (
        SparkSession.builder.appName("iceberg-overwrite-demo")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse_path)
        .config("spark.jars", str(runtime_jar))
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def create_table(spark: SparkSession) -> None:
    """Create the Iceberg table with a daily partition."""
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (
            order_id STRING,
            customer_id STRING,
            order_date DATE,
            status STRING,
            total_amount DOUBLE,
            updated_at TIMESTAMP,
            ingest_batch_id INT
        )
        USING iceberg
        PARTITIONED BY (order_date)
        TBLPROPERTIES (
            'commit.retry.num-retries'='1',
            'format-version'='2'
        )
        """
    )


def micro_batches() -> Iterable[Dict[str, object]]:
    """Return successive batches with overlapping records for the same partition."""
    return [
        {
            "batch_id": 1,
            "description": "Initial daily snapshot with two fresh orders",
            "rows": [
                {
                    "order_id": "ORD-1001",
                    "customer_id": "C-01",
                    "order_date": "2024-07-01",
                    "status": "CREATED",
                    "total_amount": 125.50,
                    "updated_at": "2024-07-01T09:15:00",
                },
                {
                    "order_id": "ORD-1002",
                    "customer_id": "C-02",
                    "order_date": "2024-07-01",
                    "status": "CREATED",
                    "total_amount": 89.10,
                    "updated_at": "2024-07-01T09:30:00",
                },
                {
                    "order_id": "ORD-1003",
                    "customer_id": "C-03",
                    "order_date": "2024-07-02",
                    "status": "CREATED",
                    "total_amount": 210.00,
                    "updated_at": "2024-07-02T08:00:00",
                },
            ],
        },
        {
            "batch_id": 2,
            "description": "Near-real-time update with adjustments for July 1st orders",
            "rows": [
                {
                    "order_id": "ORD-1001",
                    "customer_id": "C-01",
                    "order_date": "2024-07-01",
                    "status": "SHIPPED",
                    "total_amount": 125.50,
                    "updated_at": "2024-07-01T12:10:00",
                },
                {
                    "order_id": "ORD-1002",
                    "customer_id": "C-02",
                    "order_date": "2024-07-01",
                    "status": "CANCELLED",
                    "total_amount": 0.0,
                    "updated_at": "2024-07-01T12:25:00",
                },
                {
                    "order_id": "ORD-1004",
                    "customer_id": "C-04",
                    "order_date": "2024-07-01",
                    "status": "CREATED",
                    "total_amount": 55.75,
                    "updated_at": "2024-07-01T12:45:00",
                },
            ],
        },
        {
            "batch_id": 3,
            "description": "Overlap across two partitions with one late-arriving order",
            "rows": [
                {
                    "order_id": "ORD-1003",
                    "customer_id": "C-03",
                    "order_date": "2024-07-02",
                    "status": "DELIVERED",
                    "total_amount": 210.00,
                    "updated_at": "2024-07-02T14:05:00",
                },
                {
                    "order_id": "ORD-1005",
                    "customer_id": "C-05",
                    "order_date": "2024-07-02",
                    "status": "CREATED",
                    "total_amount": 130.40,
                    "updated_at": "2024-07-02T09:10:00",
                },
                {
                    "order_id": "ORD-0999",
                    "customer_id": "C-99",
                    "order_date": "2024-06-30",
                    "status": "CREATED",
                    "total_amount": 77.77,
                    "updated_at": "2024-07-02T15:00:00",
                },
            ],
        },
    ]


def transform_batch(spark: SparkSession, batch: Dict[str, object]) -> DataFrame:
    """Convert raw batch rows into a typed DataFrame ready for Iceberg."""
    raw_df = spark.createDataFrame(batch["rows"])  # Infer schema from literals

    return (
        raw_df
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("ingest_batch_id", F.lit(batch["batch_id"]))
        .select("order_id", "customer_id", "order_date", "status", "total_amount", "updated_at", "ingest_batch_id")
    )


def write_batch(df: DataFrame) -> None:
    """Overwrite the Iceberg partitions touched by the batch."""
    (
        df.writeTo(TABLE_NAME)
        .overwritePartitions()
    )


def log_current_state(spark: SparkSession, batch_id: int, description: str) -> None:
    """Print table contents and snapshots after writing a batch."""
    print("")
    print(f"===== Table contents after batch {batch_id}: {description} =====")
    spark.table(TABLE_NAME).orderBy("order_date", "order_id").show(truncate=False)

    print("")
    print("Current snapshots:")
    spark.sql(f"SELECT snapshot_id, parent_id, committed_at, operation FROM {TABLE_NAME}.snapshots ORDER BY committed_at")\
        .show(truncate=False)


def run_demo(
    warehouse_dir: Path | None = None,
    runtime_jar: Path | None = None,
    verbose: bool = True,
    log_level: str = "INFO",
) -> list[dict[str, object]]:
    """Execute the overwrite workflow and return the final table rows as dicts."""
    resolved_dir = resolve_warehouse_dir(warehouse_dir)
    resolved_jar = resolve_runtime_jar(runtime_jar)
    spark = build_spark_session(resolved_dir, resolved_jar)

    try:
        spark.sparkContext.setLogLevel(log_level)
        create_table(spark)

        for batch in micro_batches():
            df = transform_batch(spark, batch)

            if verbose:
                print("")
                print(f"Writing batch {batch['batch_id']}: {batch['description']}")

            write_batch(df)

            if verbose:
                log_current_state(spark, batch["batch_id"], batch["description"])

        if verbose:
            print("")
            print("Final table statistics:")
            spark.sql(f"DESCRIBE TABLE {TABLE_NAME}").show(truncate=False)

        final_rows = [row.asDict() for row in spark.table(TABLE_NAME).orderBy("order_date", "order_id").collect()]
        return final_rows

    finally:
        spark.stop()


def main() -> None:
    log_level = os.environ.get("ICEBERG_LOG_LEVEL", "INFO")
    run_demo(log_level=log_level)


if __name__ == "__main__":
    main()
