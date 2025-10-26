from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.errors.exceptions.base import PySparkRuntimeError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_iceberg_overwrite_demo as demo


@pytest.mark.integration
def test_run_demo_overwrites_partitions(tmp_path: Path) -> None:
    java_home = PROJECT_ROOT / "vendor" / "amazon-corretto-17.jdk" / "Contents" / "Home"
    if not java_home.exists():
        pytest.skip("Java runtime not available for Spark test")

    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = f"{java_home / 'bin'}:{os.environ['PATH']}"

    try:
        runtime_jar = demo.resolve_runtime_jar()
    except FileNotFoundError:
        pytest.skip("Iceberg runtime JAR not available for Spark test")

    # Run the Spark job against a temporary warehouse so the test is isolated.
    try:
        final_rows = demo.run_demo(
            warehouse_dir=tmp_path,
            runtime_jar=runtime_jar,
            verbose=False,
        )
    except PySparkRuntimeError as exc:
        message = str(exc).lower()
        if "java_gateway_exited" in message or "failed to bind" in message:
            pytest.skip("PySpark gateway cannot be started in this sandbox")
        raise

    assert len(final_rows) == 6

    # Map results by order id for easy assertions.
    results = {row["order_id"]: row for row in final_rows}

    assert results.keys() == {"ORD-0999", "ORD-1001", "ORD-1002", "ORD-1003", "ORD-1004", "ORD-1005"}

    assert results["ORD-1001"]["status"] == "SHIPPED"
    assert results["ORD-1002"]["status"] == "CANCELLED"
    assert results["ORD-1003"]["status"] == "DELIVERED"

    # Ensure partitions reflect the latest batch writing across dates.
    partition_dates = {row["order_date"] for row in final_rows}
    assert partition_dates == {
        results["ORD-0999"]["order_date"],
        results["ORD-1001"]["order_date"],
        results["ORD-1003"]["order_date"],
    }

    # Confirm ingest batch IDs correspond to the batch that last touched the order.
    assert results["ORD-1001"]["ingest_batch_id"] == 2
    assert results["ORD-1003"]["ingest_batch_id"] == 3
    assert results["ORD-0999"]["ingest_batch_id"] == 3
