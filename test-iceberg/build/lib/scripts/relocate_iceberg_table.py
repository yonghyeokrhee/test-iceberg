"""Rewrite Iceberg metadata files so they reference an S3 location instead of local paths.

Spark writes absolute file paths into the Iceberg metadata and manifest files.
When you rsync those files to S3 the Data Catalog (Athena, Glue, EMR) still sees
the original local paths and cannot open the table. This helper updates the
metadata JSON, manifest lists, and manifest files in-place so every file path
points at the desired S3 prefix before you sync to the cloud.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Set, Tuple

from fastavro import reader, writer

# ---------------------------------------------------------------------------
# CLI parsing


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rewrite Iceberg metadata paths for S3.")
    parser.add_argument(
        "--metadata-json",
        type=Path,
        default=Path("warehouse/order_history/metadata/v4.metadata.json"),
        help="Path to the latest Iceberg metadata JSON file.",
    )
    parser.add_argument(
        "--target-base",
        required=True,
        help="S3 URI where the table will live (e.g. s3://bucket/warehouse/order_history).",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Keep the original metadata and manifest files (.bak copies) instead of overwriting in place.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Helpers


def normalize_s3_uri(uri: str) -> str:
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI must start with s3://, got: {uri}")
    return uri.rstrip("/")


def make_mapper(source_base: str, target_base: str) -> Callable[[str], str]:
    source_base = source_base.rstrip("/")

    def mapper(path: str) -> str:
        if not path.startswith(source_base):
            # Allow paths that are already remote (for idempotency).
            if path.startswith(target_base):
                return path
            raise ValueError(f"Path '{path}' does not start with expected base '{source_base}'")
        relative = path[len(source_base) :].lstrip("/")
        return f"{target_base}/{relative}" if relative else target_base

    return mapper


def rewrite_metadata_json(metadata_path: Path, data: Dict[str, object], mapper: Callable[[str], str], *, backup: bool) -> None:
    if backup:
        shutil.copy2(metadata_path, metadata_path.with_suffix(metadata_path.suffix + ".bak"))

    data["location"] = mapper(str(data["location"]))

    for snapshot in data.get("snapshots", []):
        snapshot["manifest-list"] = mapper(snapshot["manifest-list"])

    for entry in data.get("metadata-log", []):
        entry["metadata-file"] = mapper(entry["metadata-file"])

    metadata_path.write_text(json.dumps(data, indent=2) + "\n")


def rewrite_manifest_list(path: Path, mapper: Callable[[str], str], *, backup: bool) -> Set[str]:
    manifests: Set[str] = set()
    records, schema = _read_avro(path)

    for record in records:
        original = record["manifest_path"]
        record["manifest_path"] = mapper(original)
        manifests.add(original)

    _write_avro(path, schema, records, backup=backup)
    return manifests


def rewrite_manifest(path: Path, mapper: Callable[[str], str], *, backup: bool) -> None:
    records, schema = _read_avro(path)

    for record in records:
        data_file = record["data_file"]
        data_file["file_path"] = mapper(data_file["file_path"])
        record["data_file"] = data_file

    _write_avro(path, schema, records, backup=backup)


def _read_avro(path: Path) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    with path.open("rb") as src:
        avro_reader = reader(src)
        schema = avro_reader.writer_schema
        rows = [row for row in avro_reader]
    return rows, schema


def _write_avro(path: Path, schema: Dict[str, object], rows: Iterable[Dict[str, object]], *, backup: bool) -> None:
    if backup:
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))

    with path.open("wb") as dst:
        writer(dst, schema, rows)

    crc = path.with_name(f".{path.name}.crc")
    if crc.exists():
        crc.unlink()


# ---------------------------------------------------------------------------
# Main workflow


def main() -> None:
    args = parse_args()
    metadata_path = args.metadata_json
    target_base = normalize_s3_uri(args.target_base)

    if not metadata_path.is_file():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    metadata = json.loads(metadata_path.read_text())
    source_base = metadata["location"]
    mapper = make_mapper(source_base, target_base)
    manifest_lists = _collect_manifest_lists(metadata)

    rewrite_metadata_json(metadata_path, metadata, mapper, backup=args.backup)
    manifests_to_update: Set[str] = set()

    for manifest_list in manifest_lists:
        manifests_to_update.update(rewrite_manifest_list(manifest_list, mapper, backup=args.backup))

    for manifest_path in manifests_to_update:
        rewrite_manifest(Path(manifest_path), mapper, backup=args.backup)

    print(f"Rewrote metadata to use base '{target_base}'. Now re-sync the warehouse to S3.")


def _collect_manifest_lists(metadata: Dict[str, object]) -> Set[Path]:
    return {Path(snapshot["manifest-list"]) for snapshot in metadata.get("snapshots", [])}


if __name__ == "__main__":
    main()
