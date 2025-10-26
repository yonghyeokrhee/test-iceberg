"""Worker script that overwrites a single partition, used to simulate conflicts."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.errors.exceptions.base import PySparkRuntimeError

from scripts import run_iceberg_overwrite_demo as base_demo
from scripts.concurrent_utils import amplify_rows, conflicting_batches, overwrite_conflicting_partition


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a single overwrite batch for concurrency tests.")
    parser.add_argument(
        "--batch-id",
        type=int,
        required=True,
        choices=[batch["batch_id"] for batch in conflicting_batches()],
        help="Which conflicting batch to write (101 or 102).",
    )
    parser.add_argument(
        "--warehouse-dir",
        type=str,
        help="Optional override for the Iceberg warehouse directory.",
    )
    parser.add_argument(
        "--runtime-jar",
        type=Path,
        help="Optional override for the Iceberg runtime JAR.",
    )
    parser.add_argument(
        "--log-level",
        default="WARN",
        help="Spark log level (default: WARN).",
    )
    parser.add_argument(
        "--start-at",
        type=float,
        help="Unix epoch seconds when the commit should start. Worker sleeps until this point.",
    )
    parser.add_argument(
        "--post-delay",
        type=float,
        default=0.0,
        help="Optional sleep (seconds) after finishing to keep the process alive for debugging.",
    )
    parser.add_argument(
        "--row-repeat",
        type=int,
        default=1,
        help="Duplicate each input row this many times to slow down the write.",
    )
    return parser.parse_args()


def get_batch(batch_id: int) -> dict[str, object]:
    for batch in conflicting_batches():
        if batch["batch_id"] == batch_id:
            return batch
    raise ValueError(f"Unknown batch_id {batch_id}")


def main() -> None:
    args = parse_args()

    resolved_dir = base_demo.resolve_warehouse_dir(args.warehouse_dir)
    resolved_jar = base_demo.resolve_runtime_jar(args.runtime_jar)
    spark = base_demo.build_spark_session(resolved_dir, resolved_jar)

    try:
        spark.sparkContext.setLogLevel(args.log_level)
        batch = get_batch(args.batch_id)
        df = base_demo.transform_batch(spark, batch)
        df = amplify_rows(df, args.row_repeat)

        if args.start_at:
            while time.time() < args.start_at:
                time.sleep(0.05)

        overwrite_conflicting_partition(df)
        print(f"Batch {args.batch_id} committed successfully.")

        if args.post_delay:
            time.sleep(args.post_delay)
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, Py4JJavaError, PySparkRuntimeError) as exc:
        raise SystemExit(f"Worker failed: {exc}") from exc
