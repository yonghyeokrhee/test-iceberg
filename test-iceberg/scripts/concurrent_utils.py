"""Shared helper functions/data for concurrent overwrite demos."""

from __future__ import annotations

from typing import Dict, Iterable

from pyspark.sql import DataFrame, functions as F

from scripts import run_iceberg_overwrite_demo as base_demo

CONFLICT_PARTITION = "2024-07-01"


def conflicting_batches() -> Iterable[Dict[str, object]]:
    """Return two batches that target the same partition to force a conflict."""
    return [
        {
            "batch_id": 101,
            "description": "Concurrent writer A updating July 1 orders",
            "rows": [
                {
                    "order_id": "ORD-1001",
                    "customer_id": "C-01",
                    "order_date": "2024-07-01",
                    "status": "DELIVERED",
                    "total_amount": 125.50,
                    "updated_at": "2024-07-01T15:05:00",
                },
                {
                    "order_id": "ORD-1002",
                    "customer_id": "C-02",
                    "order_date": "2024-07-01",
                    "status": "RETURNED",
                    "total_amount": 0.0,
                    "updated_at": "2024-07-01T15:15:00",
                },
            ],
        },
        {
            "batch_id": 102,
            "description": "Concurrent writer B updating July 1 orders with different states",
            "rows": [
                {
                    "order_id": "ORD-1001",
                    "customer_id": "C-01",
                    "order_date": "2024-07-01",
                    "status": "IN_TRANSIT",
                    "total_amount": 125.50,
                    "updated_at": "2024-07-01T15:06:00",
                },
                {
                    "order_id": "ORD-1004",
                    "customer_id": "C-04",
                    "order_date": "2024-07-01",
                    "status": "DELIVERED",
                    "total_amount": 55.75,
                    "updated_at": "2024-07-01T15:10:00",
                },
            ],
        },
    ]


def overwrite_conflicting_partition(df: DataFrame, partition_value: str = CONFLICT_PARTITION) -> None:
    """Overwrite only the target partition using a row-level filter to trigger optimistic locking."""
    condition = F.col("order_date") == F.to_date(F.lit(partition_value))
    (
        df.writeTo(base_demo.TABLE_NAME)
        .overwrite(condition)
    )


def amplify_rows(df: DataFrame, repeat_factor: int) -> DataFrame:
    """Duplicate each row repeat_factor times to slow down commits."""
    if repeat_factor <= 1:
        return df

    duplicates = F.array_repeat(F.lit(0), repeat_factor)
    return (
        df.withColumn("_dup", F.explode(duplicates))
        .drop("_dup")
    )
