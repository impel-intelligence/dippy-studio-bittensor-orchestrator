#!/usr/bin/env python3
"""Generate per-miner counts of jobs missing image uploads."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parquet",
        default="jobs_oct27_nov1_prompt_hashed.parquet",
        help="Path to the hashed jobs parquet export.",
    )
    parser.add_argument(
        "--output",
        default="reports/missing_images_by_miner.csv",
        help="CSV path for the summary.",
    )
    return parser.parse_args()


def has_image(row: dict) -> bool:
    payload = row.get("response_payload")
    if isinstance(payload, dict):
        metadata = payload.get("callback_metadata")
        if isinstance(metadata, dict) and isinstance(metadata.get("has_image"), bool):
            return metadata["has_image"]
        image_uri = payload.get("image_uri")
        if isinstance(image_uri, str) and image_uri.strip():
            return True
    return False


def summarize(parquet_path: Path) -> tuple[list[tuple[str, dict]], int, int]:
    table = pq.read_table(parquet_path)
    rows = table.to_pylist()
    summary = defaultdict(
        lambda: {
            "total_missing": 0,
            "success": 0,
            "failed": 0,
            "pending": 0,
            "other_status": 0,
        }
    )
    missing_jobs = 0
    for row in rows:
        if has_image(row):
            continue
        missing_jobs += 1
        hotkey = row.get("miner_hotkey") or "UNKNOWN"
        status = (row.get("status") or "").lower()
        bucket = {
            "success": "success",
            "failed": "failed",
            "pending": "pending",
        }.get(status, "other_status")
        summary[hotkey]["total_missing"] += 1
        summary[hotkey][bucket] += 1
    ordered = sorted(summary.items(), key=lambda item: item[1]["total_missing"], reverse=True)
    return ordered, missing_jobs, len(rows)


def write_csv(output_path: Path, rows: list[tuple[str, dict]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "miner_hotkey",
                "missing_jobs",
                "success_missing",
                "failed_missing",
                "pending_missing",
                "other_status_missing",
            ]
        )
        for hotkey, stats in rows:
            writer.writerow(
                [
                    hotkey,
                    stats["total_missing"],
                    stats["success"],
                    stats["failed"],
                    stats["pending"],
                    stats["other_status"],
                ]
            )


def main() -> None:
    args = parse_args()
    parquet_path = Path(args.parquet)
    if not parquet_path.exists():
        raise SystemExit(f"Parquet file not found: {parquet_path}")
    summary_rows, missing_jobs, total_rows = summarize(parquet_path)
    output_path = Path(args.output)
    write_csv(output_path, summary_rows)
    print(
        f"Wrote {output_path} with {len(summary_rows)} miners. "
        f"{missing_jobs} missing-image jobs out of {total_rows} total."
    )


if __name__ == "__main__":
    main()
