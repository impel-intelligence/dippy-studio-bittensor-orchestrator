#!/usr/bin/env python3
"""Verify callback image uploads exist in Google Cloud Storage without leaking full URIs."""

from __future__ import annotations

import argparse
import csv
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pyarrow.parquet as pq
from google.api_core import exceptions as gcs_exceptions
from google.cloud import storage


@dataclass(frozen=True)
class GcsObject:
    bucket: str
    blob: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parquet",
        default="jobs_oct27_nov1_prompt_hashed.parquet",
        help="Path to the hashed jobs parquet export.",
    )
    parser.add_argument(
        "--credentials",
        default="gcs_service_account.json",
        help="Path to the Google Cloud service account key.",
    )
    parser.add_argument(
        "--output",
        default="reports/image_existence_report.csv",
        help="CSV file to write the verification results to.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optionally limit the number of rows checked (for spot checks).",
    )
    return parser.parse_args()


def parse_gs_uri(uri: str) -> Optional[GcsObject]:
    if not isinstance(uri, str) or not uri.startswith("gs://"):
        return None
    remainder = uri[5:]
    bucket, _, blob = remainder.partition("/")
    bucket = bucket.strip()
    blob = blob.strip()
    if not bucket or not blob:
        return None
    return GcsObject(bucket=bucket, blob=blob)


def iter_jobs(parquet_path: Path, limit: Optional[int]) -> Iterable[dict]:
    table = pq.read_table(parquet_path)
    rows = table.to_pylist()
    if limit is not None:
        rows = rows[:limit]
    for row in rows:
        yield row


def load_storage_client(credentials_path: Path) -> storage.Client:
    if not credentials_path.exists():
        raise SystemExit(f"Credentials file not found: {credentials_path}")
    return storage.Client.from_service_account_json(str(credentials_path))


def gather_candidate_images(rows: Iterable[dict]) -> Iterable[Tuple[dict, GcsObject]]:
    for row in rows:
        payload = row.get("response_payload")
        if not isinstance(payload, dict):
            continue
        metadata = payload.get("callback_metadata")
        has_image = bool(metadata and metadata.get("has_image"))
        image_uri = payload.get("image_uri")
        gcs_obj = parse_gs_uri(image_uri)
        if has_image and gcs_obj:
            yield row, gcs_obj


def redact_uri(uri: Optional[str]) -> str:
    if not isinstance(uri, str) or not uri:
        return ""
    return hashlib.sha256(uri.encode("utf-8")).hexdigest()


def check_objects(
    client: storage.Client, candidates: Iterable[Tuple[dict, GcsObject]]
) -> Tuple[list, Dict[GcsObject, Tuple[bool, str]]]:
    results = []
    cache: Dict[GcsObject, Tuple[bool, str]] = {}
    for row, obj in candidates:
        if obj not in cache:
            cache[obj] = _object_status(client, obj)
        exists, error = cache[obj]
        image_uri = row.get("response_payload", {}).get("image_uri")
        results.append(
            {
                "job_id": row.get("job_id"),
                "miner_hotkey": row.get("miner_hotkey"),
                "uri_hash": redact_uri(image_uri),
                "exists": exists,
                "error": error,
            }
        )
    return results, cache


def _object_status(client: storage.Client, obj: GcsObject) -> Tuple[bool, str]:
    bucket = client.bucket(obj.bucket)
    blob = bucket.blob(obj.blob)
    try:
        return blob.exists(), ""
    except gcs_exceptions.GoogleAPIError as exc:
        return False, f"GCS error: {exc}"
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"Unexpected error: {exc}"


def write_report(output_path: Path, rows: Iterable[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "job_id",
                "miner_hotkey",
                "uri_hash",
                "exists",
                "error",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.get("job_id"),
                    row.get("miner_hotkey"),
                    row.get("uri_hash"),
                    row.get("exists"),
                    row.get("error"),
                ]
            )


def main() -> None:
    args = parse_args()
    parquet_path = Path(args.parquet)
    if not parquet_path.exists():
        raise SystemExit(f"Parquet file not found: {parquet_path}")
    credentials_path = Path(args.credentials)
    client = load_storage_client(credentials_path)

    rows = list(iter_jobs(parquet_path, args.limit))
    candidates = list(gather_candidate_images(rows))
    results, _ = check_objects(client, candidates)
    output_path = Path(args.output)
    write_report(output_path, results)

    total = len(rows)
    checked = len(results)
    exists = sum(1 for row in results if row["exists"])
    missing = checked - exists
    print(
        f"Checked {checked} images (from {total} jobs). "
        f"{exists} found, {missing} missing. Report: {output_path}"
    )


if __name__ == "__main__":
    main()
