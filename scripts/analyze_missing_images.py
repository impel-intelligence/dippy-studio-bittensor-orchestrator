#!/usr/bin/env python3
"""Generate a report of successful jobs that are missing callback image data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parquet",
        default="jobs_oct27_nov1.parquet",
        help="Path to the jobs parquet export.",
    )
    parser.add_argument(
        "--bucket",
        help="Override the GCS bucket to use when composing expected URIs.",
    )
    parser.add_argument(
        "--prefix",
        help="Override the GCS prefix to use when composing expected URIs.",
    )
    parser.add_argument(
        "--output",
        default="reports/missing_job_images.csv",
        help="CSV file to write the report to.",
    )
    return parser.parse_args()


def load_env_bucket(env_path: Path) -> Optional[str]:
    if not env_path.exists():
        return None
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "CALLBACK_GCS_BUCKET":
            return value.strip() or None
    return None


def load_config_prefix(config_path: Path) -> Optional[str]:
    if not config_path.exists():
        return None
    try:
        config = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError:
        return None
    callback = config.get("callback") if isinstance(config, dict) else None
    if not isinstance(callback, dict):
        return None
    gcs = callback.get("gcs")
    if isinstance(gcs, dict):
        prefix = gcs.get("prefix")
        if isinstance(prefix, str) and prefix.strip():
            return prefix.strip()
    prefix = callback.get("prefix")
    if isinstance(prefix, str) and prefix.strip():
        return prefix.strip()
    return None


def normalize_prefix(prefix: Optional[str]) -> str:
    if not prefix:
        return ""
    return prefix.strip("/")


def has_image(response_payload: object, result_image_url: Optional[str]) -> bool:
    if isinstance(result_image_url, str) and result_image_url.strip():
        return True
    if not isinstance(response_payload, dict):
        return False
    image_uri = response_payload.get("image_uri")
    if isinstance(image_uri, str) and image_uri.strip():
        return True
    metadata = response_payload.get("callback_metadata")
    if isinstance(metadata, dict):
        return bool(metadata.get("has_image"))
    return False


def infer_suffix(row: Dict[str, object]) -> str:
    response_payload = row.get("response_payload")
    suffix: Optional[str] = None
    if isinstance(response_payload, dict):
        image_uri = response_payload.get("image_uri")
        if isinstance(image_uri, str) and image_uri.strip():
            candidate = Path(image_uri).suffix
            if candidate:
                suffix = candidate
        if not suffix:
            content_type = response_payload.get("image_content_type")
            if isinstance(content_type, str):
                suffix = {
                    "image/png": ".png",
                    "image/jpeg": ".jpg",
                    "image/jpg": ".jpg",
                    "image/webp": ".webp",
                    "image/gif": ".gif",
                }.get(content_type.lower())
    if not suffix:
        result_image_url = row.get("result_image_url")
        if isinstance(result_image_url, str) and result_image_url.strip():
            suffix = Path(result_image_url).suffix
    return suffix or ".bin"


def build_expected_uri(
    *,
    job_id: Optional[str],
    bucket: Optional[str],
    prefix: str,
    suffix: str,
) -> Optional[str]:
    if not isinstance(job_id, str) or not job_id.strip():
        return None
    blob_name = f"{prefix}/{job_id}{suffix}" if prefix else f"{job_id}{suffix}"
    if bucket:
        return f"gs://{bucket}/{blob_name}"
    return blob_name


def main() -> None:
    args = parse_args()
    parquet_path = Path(args.parquet)
    if not parquet_path.exists():
        raise SystemExit(f"Parquet file not found: {parquet_path}")

    bucket = args.bucket or load_env_bucket(Path(".env"))
    prefix = args.prefix or load_config_prefix(Path("orchestrator/config.yaml")) or "callbacks"
    prefix = normalize_prefix(prefix)

    df = pd.read_parquet(parquet_path)

    df["status_norm"] = df["status"].astype(str).str.lower()
    mask_success = df["status_norm"] == "success"
    mask_missing_image = ~df.apply(
        lambda row: has_image(row["response_payload"], row["result_image_url"]),
        axis=1,
    )
    missing_df = df[mask_success & mask_missing_image].copy()

    if missing_df.empty:
        print("No successful jobs without image data were found.")
        return

    report_rows = []
    for _, row in missing_df.iterrows():
        response_payload = row["response_payload"]
        callback_metadata: Dict[str, object] = {}
        if isinstance(response_payload, dict):
            raw_meta = response_payload.get("callback_metadata")
            if isinstance(raw_meta, dict):
                callback_metadata = raw_meta
        suffix = infer_suffix(row.to_dict())
        expected_uri = build_expected_uri(
            job_id=row.get("job_id"),
            bucket=bucket,
            prefix=prefix,
            suffix=suffix,
        )
        report_rows.append(
            {
                "job_id": row.get("job_id"),
                "status": row.get("status"),
                "callback_status": (response_payload or {}).get("status") if isinstance(response_payload, dict) else None,
                "miner_hotkey": row.get("miner_hotkey"),
                "job_type": row.get("job_type"),
                "created_at": row.get("creation_timestamp"),
                "completed_at": row.get("completed_at"),
                "callback_has_image": callback_metadata.get("has_image"),
                "callback_error": (response_payload or {}).get("error") if isinstance(response_payload, dict) else None,
                "image_uri": (response_payload or {}).get("image_uri") if isinstance(response_payload, dict) else None,
                "image_content_type": (response_payload or {}).get("image_content_type") if isinstance(response_payload, dict) else None,
                "image_size": (response_payload or {}).get("image_size") if isinstance(response_payload, dict) else None,
                "expected_gcs_uri": expected_uri,
                "response_payload": json.dumps(response_payload, separators=(",", ":")) if isinstance(response_payload, dict) else None,
            }
        )

    report_df = pd.DataFrame(report_rows)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(output_path, index=False)

    print(f"Total jobs analysed: {len(df)}")
    print(f"Successful jobs: {mask_success.sum()}")
    print(f"Successful jobs missing images: {len(report_df)}")
    print(f"Report written to {output_path}")
    print("\nSample rows:")
    print(report_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
