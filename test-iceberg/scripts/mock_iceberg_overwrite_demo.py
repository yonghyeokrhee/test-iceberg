"""Pure-Python mock of an Iceberg partition overwrite workflow.

The goal is to simulate how successive near-real-time micro-batches might
overwrite partitions in an Apache Iceberg table without needing Spark or the
Iceberg runtime. The script keeps an in-memory representation of the table
state, including snapshot metadata, and prints the contents after every batch.

Run with:

    python scripts/mock_iceberg_overwrite_demo.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, Iterable, List, Optional


@dataclass
class OrderRecord:
    order_id: str
    customer_id: str
    order_date: date
    status: str
    total_amount: float
    updated_at: datetime
    ingest_batch_id: int


@dataclass
class TableSnapshot:
    snapshot_id: int
    parent_id: Optional[int]
    committed_at: datetime
    operation: str
    summary: Dict[str, object] = field(default_factory=dict)


class MockIcebergTable:
    """Minimal in-memory model of an Iceberg table partitioned by order_date."""

    def __init__(self, name: str, partition_field: str = "order_date") -> None:
        self.name = name
        self.partition_field = partition_field
        self.partitions: Dict[date, List[OrderRecord]] = {}
        self.snapshots: List[TableSnapshot] = []
        self._next_snapshot_id = 1

    def overwrite_partitions(self, records: Iterable[OrderRecord], description: str) -> None:
        """Replace all rows within the partitions touched by ``records``."""
        partition_map: Dict[date, List[OrderRecord]] = {}
        for record in records:
            partition_map.setdefault(record.order_date, []).append(record)

        for partition, new_rows in partition_map.items():
            # Keep rows sorted for deterministic output.
            self.partitions[partition] = sorted(
                new_rows,
                key=lambda r: (r.order_id, r.updated_at),
            )

        snapshot = TableSnapshot(
            snapshot_id=self._next_snapshot_id,
            parent_id=self.snapshots[-1].snapshot_id if self.snapshots else None,
            committed_at=datetime.utcnow(),
            operation="overwrite",
            summary={
                "description": description,
                "affected_partitions": [p.isoformat() for p in partition_map],
                "records_written": sum(len(rows) for rows in partition_map.values()),
            },
        )
        self.snapshots.append(snapshot)
        self._next_snapshot_id += 1

    def table_rows(self) -> List[OrderRecord]:
        all_rows: List[OrderRecord] = []
        for partition in sorted(self.partitions):
            all_rows.extend(self.partitions[partition])
        return sorted(all_rows, key=lambda r: (r.order_date, r.order_id))

    def print_table_state(self, header: str) -> None:
        print("")
        print(f"===== {header} =====")
        rows = self.table_rows()
        if not rows:
            print("(table is empty)")
            return

        for row in rows:
            print(
                f"order_date={row.order_date.isoformat()} order_id={row.order_id} "
                f"status={row.status:<9} amount={row.total_amount:>7.2f} "
                f"updated_at={row.updated_at.isoformat()} ingest_batch={row.ingest_batch_id}"
            )

    def print_snapshots(self) -> None:
        print("")
        print("Current snapshots:")
        for snapshot in self.snapshots:
            print(
                f"snapshot_id={snapshot.snapshot_id} parent_id={snapshot.parent_id} "
                f"committed_at={snapshot.committed_at.isoformat()} operation={snapshot.operation} "
                f"summary={snapshot.summary}"
            )

    def describe(self) -> None:
        total_rows = len(self.table_rows())
        total_partitions = len(self.partitions)
        print("")
        print("Final table statistics:")
        print(f"total_rows={total_rows} total_partitions={total_partitions}")


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_timestamp(value: str) -> datetime:
    # Accept ISO timestamps without timezone.
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


def micro_batches() -> Iterable[Dict[str, object]]:
    """Yield successive mock micro-batches with overlapping partitions."""
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
            "description": "Near-real-time update with adjustments for July 1 orders",
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


def convert_rows(batch: Dict[str, object]) -> List[OrderRecord]:
    records: List[OrderRecord] = []
    batch_id = batch["batch_id"]
    for row in batch["rows"]:
        records.append(
            OrderRecord(
                order_id=row["order_id"],
                customer_id=row["customer_id"],
                order_date=parse_date(row["order_date"]),
                status=row["status"],
                total_amount=float(row["total_amount"]),
                updated_at=parse_timestamp(row["updated_at"]),
                ingest_batch_id=batch_id,
            )
        )
    return records


def main() -> None:
    table = MockIcebergTable(name="order_history")

    for batch in micro_batches():
        records = convert_rows(batch)
        header = f"Writing batch {batch['batch_id']}: {batch['description']}"
        print("")
        print(header)
        table.overwrite_partitions(records, description=batch["description"])
        table.print_table_state(f"Table contents after batch {batch['batch_id']}")
        table.print_snapshots()

    table.describe()


if __name__ == "__main__":
    main()
