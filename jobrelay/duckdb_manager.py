"""DuckDB management utilities coordinating cache and sync behaviour."""

from __future__ import annotations

import json
import logging
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from uuid import UUID

import duckdb
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - fail fast when dependency missing
    raise ImportError("pyarrow is required for JobRelay DuckDB snapshots") from exc

from .config import Settings


LOGGER = logging.getLogger("jobrelay.duckdb_manager")


class JobRelayDuckDBManager:
    """Owns DuckDB connections, schema management, and GCS sync plumbing."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._db_path = settings.resolved_cache_db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_conn = duckdb.connect(str(self._db_path))
        self._configure_connection()
        self._ensure_schema()
        self._maybe_restore_from_snapshot()

    def _configure_connection(self) -> None:
        try:
            self._write_conn.execute("PRAGMA busy_timeout=5000")
        except duckdb.Error as exc:  # pragma: no cover - pragma optional across versions
            LOGGER.debug("Skipping busy_timeout pragma: %s", exc)
        self._write_conn.execute(
            f"SET memory_limit='{self._settings.duckdb_memory_limit}'"
        )
        self._write_conn.execute(f"SET threads={self._settings.duckdb_threads}")

    def _ensure_schema(self) -> None:
        self._write_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inference_jobs (
                job_id UUID PRIMARY KEY,
                job_type TEXT NOT NULL,
                miner_hotkey TEXT NOT NULL,
                payload JSON NOT NULL,
                result_image_url TEXT,
                creation_timestamp TIMESTAMPTZ NOT NULL,
                last_updated_at TIMESTAMPTZ,
                miner_received_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                execution_duration_ms INTEGER,
                expires_at TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL,
                audit_status TEXT NOT NULL,
                verification_status TEXT NOT NULL,
                is_audit_job BOOLEAN NOT NULL,
                audit_target_job_id UUID,
                prepared_at TIMESTAMPTZ,
                dispatched_at TIMESTAMPTZ,
                failure_reason TEXT,
                response_payload JSON,
                response_timestamp TIMESTAMPTZ,
                callback_secret TEXT,
                prompt_seed BIGINT
            )
            """
        )
        self._write_conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_inference_jobs_miner_hotkey
            ON inference_jobs(miner_hotkey)
            """
        )
        self._write_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS duckdb_sync_state (
                id INTEGER PRIMARY KEY,
                last_snapshot TIMESTAMPTZ
            )
            """
        )
        self._write_conn.execute(
            """
            INSERT INTO duckdb_sync_state (id, last_snapshot)
            SELECT 1, TIMESTAMPTZ '1970-01-01 00:00:00+00'
            WHERE NOT EXISTS (SELECT 1 FROM duckdb_sync_state WHERE id = 1)
            """
        )
        self._ensure_additional_columns()

    def _ensure_additional_columns(self) -> None:
        expected = {
            "prepared_at": "TIMESTAMPTZ",
            "dispatched_at": "TIMESTAMPTZ",
            "failure_reason": "TEXT",
            "response_payload": "JSON",
            "response_timestamp": "TIMESTAMPTZ",
            "callback_secret": "TEXT",
            "prompt_seed": "BIGINT",
        }
        existing = {
            row[1]
            for row in self._write_conn.execute("PRAGMA table_info('inference_jobs')").fetchall()
        }
        for column, ddl in expected.items():
            if column not in existing:
                self._write_conn.execute(
                    f"ALTER TABLE inference_jobs ADD COLUMN {column} {ddl}"
                )

    def _maybe_restore_from_snapshot(self) -> None:
        if not self._settings.gcs_bucket:
            raise ValueError("JOBRELAY_GCS_BUCKET must be configured for snapshot management")
        if self._has_existing_data():
            LOGGER.debug("Local DuckDB already contains data; skipping snapshot restore")
            return
        try:
            snapshot = self._fetch_latest_snapshot_metadata()
        except FileNotFoundError:
            LOGGER.info("No snapshot found in gs://%s/%s", self._settings.gcs_bucket, self._settings.gcs_prefix)
            return
        if snapshot is None:
            LOGGER.info("No snapshot found in gs://%s/%s", self._settings.gcs_bucket, self._settings.gcs_prefix)
            return
        blob, snapshot_time = snapshot
        local_file = self._download_snapshot(blob)
        try:
            df = pd.read_parquet(local_file, engine="pyarrow")
        finally:
            local_file.unlink(missing_ok=True)
        if df.empty:
            LOGGER.info("Snapshot %s was empty; continuing with fresh database", blob.name)
            return
        snapshot_epoch = None
        if "snapshot_epoch" in df.columns:
            snapshot_epoch = df["snapshot_epoch"].dropna().max()
            df = df.drop(columns=["snapshot_epoch"])
        columns_order = [
            "job_id",
            "job_type",
            "miner_hotkey",
            "payload",
            "result_image_url",
            "creation_timestamp",
            "last_updated_at",
            "miner_received_at",
            "completed_at",
            "execution_duration_ms",
            "expires_at",
            "status",
            "audit_status",
            "verification_status",
            "is_audit_job",
            "audit_target_job_id",
        ]
        allowed_columns = set(columns_order)
        missing_columns = allowed_columns - set(df.columns)
        if missing_columns:
            raise RuntimeError(
                f"Snapshot {blob.name} is missing required columns: {sorted(missing_columns)}"
            )
        df = df[columns_order]
        df["job_id"] = df["job_id"].astype(str)
        if "audit_target_job_id" in df.columns:
            df["audit_target_job_id"] = df["audit_target_job_id"].astype("string")
        self._write_conn.execute("DELETE FROM inference_jobs")
        self._write_conn.register("snapshot_restore", df)
        columns_csv = ", ".join(columns_order)
        self._write_conn.execute(
            f"INSERT INTO inference_jobs ({columns_csv}) SELECT {columns_csv} FROM snapshot_restore"
        )
        self._write_conn.unregister("snapshot_restore")
        if snapshot_epoch is not None and hasattr(snapshot_epoch, "to_pydatetime"):
            snapshot_epoch = snapshot_epoch.to_pydatetime()
        if snapshot_epoch is not None and snapshot_epoch.tzinfo is None:
            snapshot_epoch = snapshot_epoch.replace(tzinfo=timezone.utc)
        restore_timestamp = snapshot_epoch or snapshot_time
        self._set_last_snapshot_time(restore_timestamp)
        LOGGER.info(
            "Restored %s inference job records from %s",
            len(df.index),
            blob.name,
        )

    def insert_job(self, record: Dict[str, object]) -> None:
        payload = json.dumps(record["payload"], separators=(",", ":"))
        parameters = {**record, "payload": payload}
        if "response_payload" in parameters and parameters["response_payload"] is not None:
            parameters["response_payload"] = json.dumps(
                parameters["response_payload"], separators=(",", ":")
            )
        self._write_conn.execute(
            """
            INSERT INTO inference_jobs (
                job_id, job_type, miner_hotkey, payload,
                result_image_url, creation_timestamp, last_updated_at,
                miner_received_at, completed_at, execution_duration_ms,
                expires_at, status, audit_status, verification_status,
                is_audit_job, audit_target_job_id,
                prepared_at, dispatched_at, failure_reason,
                response_payload, response_timestamp,
                callback_secret, prompt_seed
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                parameters["job_id"],
                parameters["job_type"],
                parameters["miner_hotkey"],
                parameters["payload"],
                parameters.get("result_image_url"),
                parameters["creation_timestamp"],
                parameters.get("last_updated_at"),
                parameters.get("miner_received_at"),
                parameters.get("completed_at"),
                parameters.get("execution_duration_ms"),
                parameters["expires_at"],
                parameters["status"],
                parameters["audit_status"],
                parameters["verification_status"],
                parameters["is_audit_job"],
                parameters.get("audit_target_job_id"),
                parameters.get("prepared_at"),
                parameters.get("dispatched_at"),
                parameters.get("failure_reason"),
                parameters.get("response_payload"),
                parameters.get("response_timestamp"),
                parameters.get("callback_secret"),
                parameters.get("prompt_seed"),
            ],
        )

    def update_job(self, job_id: UUID, updates: Dict[str, object]) -> None:
        if not updates:
            return
        assignments: List[str] = []
        parameters: List[object] = []
        for key, value in updates.items():
            if key == "payload":
                value = json.dumps(value, separators=(",", ":"))
            elif key == "response_payload" and value is not None:
                value = json.dumps(value, separators=(",", ":"))
            assignments.append(f"{key} = ?")
            parameters.append(value)
        parameters.append(job_id)
        set_clause = ", ".join(assignments)
        query = f"UPDATE inference_jobs SET {set_clause} WHERE job_id = ?"
        self._write_conn.execute(query, parameters)

    def fetch_job(self, job_id: UUID) -> Optional[Dict[str, object]]:
        with self._read_cursor() as cursor:
            cursor.execute(
                """
                SELECT job_id, job_type, miner_hotkey, payload,
                       result_image_url, creation_timestamp, last_updated_at,
                       miner_received_at, completed_at, execution_duration_ms,
                       expires_at, status, audit_status, verification_status,
                       is_audit_job, audit_target_job_id,
                       prepared_at, dispatched_at, failure_reason,
                       response_payload, response_timestamp,
                       callback_secret, prompt_seed
                FROM inference_jobs
                WHERE job_id = ?
                """,
                [job_id],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            return self._row_to_dict(columns, row)

    def fetch_all(self) -> List[Dict[str, object]]:
        with self._read_cursor() as cursor:
            cursor.execute(
                """
                SELECT job_id, job_type, miner_hotkey, payload,
                       result_image_url, creation_timestamp, last_updated_at,
                       miner_received_at, completed_at, execution_duration_ms,
                       expires_at, status, audit_status, verification_status,
                       is_audit_job, audit_target_job_id,
                       prepared_at, dispatched_at, failure_reason,
                       response_payload, response_timestamp,
                       callback_secret, prompt_seed
                FROM inference_jobs
                """
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [self._row_to_dict(columns, row) for row in rows]

    def fetch_for_hotkey(self, miner_hotkey: str) -> List[Dict[str, object]]:
        with self._read_cursor() as cursor:
            cursor.execute(
                """
                SELECT job_id, job_type, miner_hotkey, payload,
                       result_image_url, creation_timestamp, last_updated_at,
                       miner_received_at, completed_at, execution_duration_ms,
                       expires_at, status, audit_status, verification_status,
                       is_audit_job, audit_target_job_id,
                       prepared_at, dispatched_at, failure_reason,
                       response_payload, response_timestamp,
                       callback_secret, prompt_seed
                FROM inference_jobs
                WHERE miner_hotkey = ?
                """,
                [miner_hotkey],
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [self._row_to_dict(columns, row) for row in rows]

    def delete_expired(self, now: datetime) -> int:
        result = self._write_conn.execute(
            "DELETE FROM inference_jobs WHERE expires_at <= ?", [now]
        )
        return result.fetchall()[0][0] if result.description else 0

    def checkpoint(self) -> None:
        self._write_conn.execute("CHECKPOINT")

    def close(self) -> None:
        self._write_conn.close()

    def nuclear_wipe(self) -> dict[str, int]:
        """Irreversibly remove all local jobs and remote snapshots."""

        deleted_jobs = 0
        try:
            result = self._write_conn.execute("SELECT COUNT(*) FROM inference_jobs").fetchone()
            if result is not None and result[0] is not None:
                deleted_jobs = int(result[0])
        except duckdb.Error:  # pragma: no cover - defensive
            LOGGER.warning("Failed counting inference jobs before wipe", exc_info=True)

        try:
            self._write_conn.execute("DELETE FROM inference_jobs")
            self._write_conn.execute("DELETE FROM duckdb_sync_state")
            self._write_conn.execute(
                """
                INSERT INTO duckdb_sync_state (id, last_snapshot)
                VALUES (1, TIMESTAMPTZ '1970-01-01 00:00:00+00')
                """
            )
            self._write_conn.execute("CHECKPOINT")
            try:
                self._write_conn.execute("VACUUM")
            except duckdb.Error:  # pragma: no cover - VACUUM optional on versions without support
                LOGGER.debug("VACUUM unsupported or failed during nuclear wipe", exc_info=True)
        except duckdb.Error as exc:  # pragma: no cover - severe failure path
            LOGGER.exception("Failed wiping local DuckDB state: %s", exc)
            raise

        snapshots_deleted = self._delete_all_snapshots()

        return {
            "deleted_jobs": deleted_jobs,
            "snapshots_deleted": snapshots_deleted,
        }

    def sync_to_gcs(self, snapshot_time: Optional[datetime] = None) -> Optional[str]:
        if not self._settings.gcs_bucket:
            raise ValueError("JOBRELAY_GCS_BUCKET must be configured for snapshot management")

        snapshot_time = snapshot_time or datetime.now(timezone.utc)
        self.checkpoint()
        last_snapshot = self._get_last_snapshot_time()
        params: List[object] = []
        query = "SELECT * FROM inference_jobs"
        if last_snapshot is not None:
            query += " WHERE last_updated_at IS NULL OR last_updated_at >= ?"
            params.append(last_snapshot)

        LOGGER.debug("Fetching inference_jobs for snapshot newer than %s", last_snapshot)
        batch = self._write_conn.execute(query, params).fetch_df()
        if batch.empty:
            LOGGER.info("No inference job changes since last snapshot; skipping upload")
            self._set_last_snapshot_time(snapshot_time)
            return None

        batch = batch.copy()
        batch["job_id"] = batch["job_id"].apply(str)
        if "audit_target_job_id" in batch.columns:
            batch["audit_target_job_id"] = batch["audit_target_job_id"].apply(
                lambda value: str(value)
                if value is not None and not pd.isna(value)
                else None
            )
        batch["snapshot_epoch"] = snapshot_time
        partition_key = snapshot_time.strftime("%Y%m%d%H")

        local_path = self._write_snapshot_to_parquet(batch)
        try:
            gcs_uri = self._upload_snapshot(local_path, snapshot_time)
        finally:
            try:
                local_path.unlink(missing_ok=True)
            except OSError:  # pragma: no cover - best effort cleanup
                LOGGER.debug("Temporary snapshot file cleanup failed", exc_info=True)

        self._set_last_snapshot_time(snapshot_time)
        LOGGER.info(
            "Uploaded %s inference job records to %s",
            len(batch.index),
            gcs_uri,
        )
        self._enforce_cache_budget()
        return gcs_uri

    @contextmanager
    def _read_cursor(self):
        conn = duckdb.connect(str(self._db_path))
        try:
            cursor = conn.cursor()
            yield cursor
        finally:
            conn.close()

    @staticmethod
    def _row_to_dict(columns: Iterable[str], row: Iterable[object]) -> Dict[str, object]:
        as_dict = dict(zip(columns, row))
        payload = as_dict.get("payload")
        if isinstance(payload, str):
            as_dict["payload"] = json.loads(payload)
        response_payload = as_dict.get("response_payload")
        if isinstance(response_payload, str):
            as_dict["response_payload"] = json.loads(response_payload)
        return as_dict

    def _get_last_snapshot_time(self) -> Optional[datetime]:
        row = self._write_conn.execute(
            "SELECT last_snapshot FROM duckdb_sync_state WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        return row[0]

    def _set_last_snapshot_time(self, timestamp: datetime) -> None:
        self._write_conn.execute(
            "UPDATE duckdb_sync_state SET last_snapshot = ? WHERE id = 1",
            [timestamp],
        )

    def _write_snapshot_to_parquet(self, batch: pd.DataFrame) -> Path:
        compression = (self._settings.compression_type or "zstd").lower()
        row_group_size = self._settings.row_group_size
        table = pa.Table.from_pandas(batch, preserve_index=False)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            pq.write_table(
                table,
                tmp.name,
                compression=compression,
                row_group_size=row_group_size,
            )
            return Path(tmp.name)

    def _upload_snapshot(self, local_path: Path, snapshot_time: datetime) -> str:
        bucket_name = self._settings.gcs_bucket
        if not bucket_name:
            raise ValueError("GCS bucket not configured")

        strategy = (self._settings.partition_strategy or "hourly").lower()
        if strategy != "hourly":  # pragma: no cover - future strategies may be added
            LOGGER.warning("Unsupported partition strategy '%s'; defaulting to hourly", strategy)
        year = snapshot_time.strftime("%Y")
        month = snapshot_time.strftime("%m")
        day = snapshot_time.strftime("%d")
        hour = snapshot_time.strftime("%H")
        snapshot_id = snapshot_time.strftime("%Y%m%d_%H%M%S")

        prefix = self._settings.gcs_prefix.strip("/")
        blob_path = (
            f"{prefix}/year={year}/month={month}/day={day}/hour={hour}/"
            f"snapshot_{snapshot_id}.parquet"
        )

        client = self._build_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(str(local_path))
        return f"gs://{bucket_name}/{blob_path}"

    def _fetch_latest_snapshot_metadata(self):
        client = self._build_storage_client()
        prefix = (self._settings.gcs_prefix or "").strip("/")
        prefix_arg = f"{prefix}/" if prefix else None
        blobs = list(client.list_blobs(self._settings.gcs_bucket, prefix=prefix_arg))
        candidates = [blob for blob in blobs if hasattr(blob, "name") and blob.name.endswith(".parquet")]
        if not candidates:
            return None
        latest = max(candidates, key=self._snapshot_time_from_blob)
        snapshot_time = self._snapshot_time_from_blob(latest)
        return latest, snapshot_time

    def _snapshot_time_from_blob(self, blob) -> datetime:
        timestamp = getattr(blob, "time_created", None)
        if timestamp is not None:
            return timestamp.astimezone(timezone.utc)
        match = re.search(r"snapshot_(\d{8}_\d{6})", getattr(blob, "name", ""))
        if match:
            parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
            return parsed.replace(tzinfo=timezone.utc)
        raise FileNotFoundError("Unable to determine snapshot timestamp from blob metadata")

    def _download_snapshot(self, blob) -> Path:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            blob.download_to_filename(tmp.name)
            return Path(tmp.name)

    def _build_storage_client(self):
        try:
            from google.cloud import storage
        except ImportError as exc:  # pragma: no cover - fail fast when dependency missing
            raise ImportError(
                "google-cloud-storage is required for JobRelay snapshot management"
            ) from exc

        credentials = self._settings.resolved_gcp_credentials
        if credentials and credentials.exists():
            return storage.Client.from_service_account_json(str(credentials))
        return storage.Client()

    def _delete_all_snapshots(self) -> int:
        bucket_name = self._settings.gcs_bucket
        if not bucket_name:
            LOGGER.warning("Skipping snapshot deletion; GCS bucket is not configured")
            return 0

        prefix = (self._settings.gcs_prefix or "").strip("/")
        prefix_arg = f"{prefix}/" if prefix else None

        try:
            client = self._build_storage_client()
        except Exception as exc:  # pragma: no cover - dependency/env failure
            LOGGER.exception("Failed to build storage client for nuclear wipe: %s", exc)
            return 0

        deleted = 0
        for blob in client.list_blobs(bucket_name, prefix=prefix_arg):
            try:
                blob.delete()
                deleted += 1
            except Exception:  # pragma: no cover - best effort deletion
                LOGGER.exception("Failed deleting snapshot blob %s", getattr(blob, "name", "<unknown>"))
        return deleted

    def _has_existing_data(self) -> bool:
        result = self._write_conn.execute("SELECT COUNT(*) FROM inference_jobs").fetchone()
        return bool(result and result[0])

    def _enforce_cache_budget(self) -> None:
        size_mb = self._estimate_cache_size_mb()
        limit_mb = float(self._settings.cache_max_size_gb) * 1024
        if size_mb > limit_mb:
            LOGGER.warning(
                "DuckDB cache is %.1f MB which exceeds configured limit %.1f MB",
                size_mb,
                limit_mb,
            )

    def _estimate_cache_size_mb(self) -> float:
        try:
            return self._db_path.stat().st_size / (1024 * 1024)
        except FileNotFoundError:
            return 0.0
