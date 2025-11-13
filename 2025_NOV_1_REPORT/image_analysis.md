# Image Export Analysis



## 0. Hashed Parquet Export

- Tooling: `uv run python` with `pyarrow`.
- Dataset: `jobs_oct27_nov1_prompt_hashed.parquet` (already hashed; treat it as the canonical starting point).
- Steps:
  1. Install PyArrow (`uv pip install pyarrow`) if it is not already in `.venv`.
  2. Load the file to view schema, row counts (7,884 rows, 23 columns), and timestamp ranges.
  3. Note the nested structs: `payload` (request metadata with hashed prompt) and `response_payload` (callback metadata, image info, runtimes, etc.).

## 1. Summarize Missing Images per Miner

- Script: `generate_missing_images_summary.py`.
- Logic: a job is “missing” when `response_payload.callback_metadata.has_image` is `False`, or when no valid `image_uri` is present.
- Command:
  ```bash
  uv run python generate_missing_images_summary.py \
      --parquet jobs_oct27_nov1_prompt_hashed.parquet \
      --output reports/missing_images_by_miner.csv
  ```
- Totals observed: 3,779 jobs without images across 59 miners.

## 3. Verify Uploaded Images Exist in GCS

- Dependencies: `uv pip install google-cloud-storage`.
- Script: `check_gcs_images.py`.
- Usage:
  ```bash
  uv run python check_gcs_images.py \
      --parquet jobs_oct27_nov1_prompt_hashed.parquet \
      --credentials gcs_service_account.json \
      --output reports/image_existence_report.csv
  ```
- Behavior:
  1. Scans jobs whose callback metadata claims an image and collects their `gs://` URIs.
  2. Checks object existence via the GCS API (duplicate URIs are cached).
  3. Writes `job_id`, `miner_hotkey`, a SHA-256 **hash** of the URI, and the `exists/error` fields—so no bucket or blob names leave the report.
- Recent sample run (`--limit 100`): 61 referenced images checked, all present (report only exposes hashed identifiers).

## 4. Flag Suspect Miners via Success-without-Image Ratio

- Script: `flag_missing_image_miners.py`.
- Method:
  - Compute ratio = `success_missing / missing_jobs`.
  - Flag miners when ratio ≥ 0.5 **and** `missing_jobs ≥ 50`.
- Command:
  ```bash
  uv run python reports/flag_missing_image_miners.py \
      --summary reports/missing_images_by_miner.csv \
      --output reports/missing_images_flagged.csv \
      --success-ratio 0.5 \
      --min-missing 50
  ```
- This produced 29 suspect miners
