#!/usr/bin/env python3
"""Flag miners whose missing-image jobs are mostly marked as success."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--summary",
        default="reports/missing_images_by_miner.csv",
        help="Path to the CSV produced by generate_missing_images_summary.py.",
    )
    parser.add_argument(
        "--output",
        default="reports/missing_images_flagged.csv",
        help="CSV file for flagged miners.",
    )
    parser.add_argument(
        "--min-missing",
        type=int,
        default=50,
        help="Minimum missing-image jobs required to consider a miner.",
    )
    parser.add_argument(
        "--success-ratio",
        type=float,
        default=0.5,
        help="Minimum success_missing / missing_jobs ratio to flag a miner.",
    )
    return parser.parse_args()


def load_summary(summary_path: Path) -> list[dict]:
    if not summary_path.exists():
        raise SystemExit(f"Summary CSV not found: {summary_path}")
    with summary_path.open() as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def flag_miners(rows: list[dict], min_missing: int, ratio_threshold: float) -> list[dict]:
    flagged: list[dict] = []
    for row in rows:
        missing = int(row["missing_jobs"])
        success_missing = int(row["success_missing"])
        ratio = success_missing / missing if missing else 0.0
        if missing >= min_missing and ratio >= ratio_threshold:
            flagged.append(
                {
                    "miner_hotkey": row["miner_hotkey"],
                    "missing_jobs": missing,
                    "success_missing": success_missing,
                    "success_missing_ratio": f"{ratio:.4f}",
                }
            )
    return flagged


def write_csv(output_path: Path, flagged: list[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "miner_hotkey",
                "missing_jobs",
                "success_missing",
                "success_missing_ratio",
            ],
        )
        writer.writeheader()
        writer.writerows(flagged)


def main() -> None:
    args = parse_args()
    rows = load_summary(Path(args.summary))
    flagged = flag_miners(rows, args.min_missing, args.success_ratio)
    output_path = Path(args.output)
    write_csv(output_path, flagged)
    print(
        f"Flagged {len(flagged)} miners "
        f"(ratio>= {args.success_ratio}, missing>= {args.min_missing}) "
        f"-> {output_path}"
    )


if __name__ == "__main__":
    main()
