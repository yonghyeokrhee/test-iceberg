"""Demonstrate concurrent overwrite writers on the same Iceberg partition.

Two writers attempt to overwrite the `order_history` table at the same time,
targeting the same `order_date` partition. One writer should succeed while the
other fails with a concurrency exception, illustrating Iceberg's optimistic
locking behavior.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List

from py4j.protocol import Py4JJavaError
from pyspark.errors.exceptions.base import PySparkRuntimeError

from scripts import run_iceberg_overwrite_demo as base_demo
from scripts.concurrent_utils import (
    CONFLICT_PARTITION,
    amplify_rows,
    conflicting_batches,
    overwrite_conflicting_partition,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run concurrent overwrite writers against Iceberg.")
    parser.add_argument(
        "--warehouse-dir",
        type=str,
        help="Optional override for the Iceberg warehouse directory.",
    )
    parser.add_argument(
        "--runtime-jar",
        type=Path,
        help="Optional override for the Iceberg Spark runtime JAR.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Spark log level (default: INFO).",
    )
    parser.add_argument(
        "--warmup-batches",
        type=int,
        default=1,
        help="Number of sequential batches to load before triggering the conflict (default: 1).",
    )
    parser.add_argument(
        "--pre-commit-delay",
        type=float,
        default=0.5,
        help="Seconds to sleep after the barrier before committing, to amplify the overlap (default: 0.5).",
    )
    parser.add_argument(
        "--mode",
        choices=["threads", "processes"],
        default="threads",
        help="How to coordinate the concurrent writers (default: threads in a single Spark app).",
    )
    parser.add_argument(
        "--process-start-delay",
        type=float,
        default=3.0,
        help="When mode=processes, wait this many seconds before both workers start committing.",
    )
    parser.add_argument(
        "--row-repeat",
        type=int,
        default=1,
        help="Duplicate each row this many times to prolong writes and increase conflict odds.",
    )
    return parser.parse_args()


def run_concurrent_demo(
    *,
    warehouse_dir: Path | None = None,
    runtime_jar: Path | None = None,
    log_level: str = "INFO",
    warmup_batches: int = 1,
    pre_commit_delay: float = 0.5,
    mode: str = "threads",
    process_start_delay: float = 3.0,
    row_repeat: int = 1,
) -> List[Dict[str, object]]:
    resolved_dir = base_demo.resolve_warehouse_dir(warehouse_dir)
    resolved_jar = base_demo.resolve_runtime_jar(runtime_jar)
    spark = base_demo.build_spark_session(resolved_dir, resolved_jar)

    try:
        spark.sparkContext.setLogLevel(log_level)
        base_demo.create_table(spark)
        spark.sql(
            f"ALTER TABLE {base_demo.TABLE_NAME} SET TBLPROPERTIES ('commit.retry.num-retries'='0')"
        )

        for batch in base_demo.micro_batches()[:warmup_batches]:
            df = base_demo.transform_batch(spark, batch)
            print("")
            print(f"Loading warmup batch {batch['batch_id']}: {batch['description']}")
            base_demo.write_batch(df)

        base_demo.log_current_state(spark, 0, "After warmup batches")

        print("")
        print("Starting concurrent writers...")
        if mode == "threads":
            results = execute_concurrent_writes(
                spark,
                conflicting_batches(),
                pre_commit_delay=pre_commit_delay,
                row_repeat=row_repeat,
            )
        else:
            results = execute_process_writes(
                resolved_dir=resolved_dir,
                runtime_jar=resolved_jar,
                log_level=log_level,
                start_delay=process_start_delay,
                row_repeat=row_repeat,
            )

        print("")
        print("Concurrent write results:")
        for result in results:
            status = result["status"]
            message = result.get("message")
            if status == "succeeded":
                print(f"- Batch {result['batch_id']} succeeded.")
            else:
                print(f"- Batch {result['batch_id']} failed: {message}")

        base_demo.log_current_state(spark, 999, "After concurrent writers (if any succeeded)")

        return results
    finally:
        spark.stop()


def execute_concurrent_writes(
    spark,
    batches: Iterable[Dict[str, object]],
    *,
    pre_commit_delay: float,
    row_repeat: int,
) -> List[Dict[str, object]]:
    batches = list(batches)
    barrier = threading.Barrier(len(batches))
    results: List[Dict[str, object]] = []

    def worker(batch: Dict[str, object]) -> Dict[str, object]:
        session = spark.newSession()
        df = base_demo.transform_batch(session, batch)
        df = amplify_rows(df, row_repeat)
        print(f"Batch {batch['batch_id']} ready; waiting for barrier to align commits...")

        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            return {
                "batch_id": batch["batch_id"],
                "status": "failed",
                "message": "Barrier broken before commit; other writer crashed.",
            }

        if pre_commit_delay:
            time.sleep(pre_commit_delay)

        try:
            overwrite_conflicting_partition(df)
            return {"batch_id": batch["batch_id"], "status": "succeeded"}
        except Py4JJavaError as exc:  # Iceberg throws through Py4J
            return {
                "batch_id": batch["batch_id"],
                "status": "failed",
                "message": str(exc.java_exception) if exc.java_exception else str(exc),
            }
        finally:
            session.catalog.clearCache()

    with ThreadPoolExecutor(max_workers=len(batches)) as executor:
        future_to_batch = {executor.submit(worker, batch): batch for batch in batches}
        for future in as_completed(future_to_batch):
            results.append(future.result())

    return sorted(results, key=lambda r: r["batch_id"])


def execute_process_writes(
    *,
    resolved_dir: str,
    runtime_jar: Path,
    log_level: str,
    start_delay: float,
    row_repeat: int,
) -> List[Dict[str, object]]:
    """Launch two independent Spark processes so their commits overlap at the OS level."""
    start_at = time.time() + max(start_delay, 0.0)
    env = os.environ.copy()
    # Ensure local repo wins over the installed package.
    repo_root = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = f"{repo_root}:{env.get('PYTHONPATH', '')}"

    worker_script = Path(__file__).resolve().parent / "concurrent_writer_worker.py"
    procs = []
    for batch in conflicting_batches():
        cmd = [
            sys.executable,
            str(worker_script),
            f"--batch-id={batch['batch_id']}",
            f"--warehouse-dir={resolved_dir}",
            f"--runtime-jar={runtime_jar}",
            f"--log-level={log_level}",
            f"--start-at={start_at}",
            f"--row-repeat={row_repeat}",
        ]
        procs.append(
            (
                batch["batch_id"],
                subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env),
            )
        )

    results: List[Dict[str, object]] = []
    for batch_id, proc in procs:
        stdout, stderr = proc.communicate()
        status = "succeeded" if proc.returncode == 0 else "failed"
        message = stderr.strip() or stdout.strip()
        results.append(
            {
                "batch_id": batch_id,
                "status": status,
                "message": message,
            }
        )

    return sorted(results, key=lambda r: r["batch_id"])


def main() -> None:
    args = parse_args()

    try:
        run_concurrent_demo(
            warehouse_dir=args.warehouse_dir,
            runtime_jar=args.runtime_jar,
            log_level=args.log_level,
            warmup_batches=args.warmup_batches,
            pre_commit_delay=args.pre_commit_delay,
            mode=args.mode,
            process_start_delay=args.process_start_delay,
             row_repeat=args.row_repeat,
        )
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc
    except (Py4JJavaError, PySparkRuntimeError) as exc:
        raise SystemExit(f"Concurrent demo failed: {exc}") from exc


if __name__ == "__main__":
    main()
