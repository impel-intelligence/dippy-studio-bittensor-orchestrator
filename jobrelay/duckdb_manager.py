"""DuckDB management utilities coordinating cache and sync behaviour."""

from __future__ import annotations

import json
import logging
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
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

    _FETCH_COLUMNS = [
        "job_id",
        "job_type",
        "miner_hotkey",
        "source",
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
        "prepared_at",
        "dispatched_at",
        "failure_reason",
        "response_payload",
        "response_timestamp",
        "callback_secret",
        "prompt_seed",
        "source",
    ]
    _SNAPSHOT_METADATA_COLUMNS = ["snapshot_epoch"]

    def __init__(self, settings: Settings):
        self._settings = settings
        self._db_path = settings.resolved_cache_db_path
        self._skip_snapshot_io = False
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
                source TEXT NOT NULL DEFAULT '',
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
            CREATE INDEX IF NOT EXISTS idx_inference_jobs_hotkey_completed
            ON inference_jobs(miner_hotkey, completed_at)
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
            "source": "TEXT",
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
        if self._skip_snapshot_io:
            LOGGER.info("Snapshot restore skipped because snapshot IO is disabled")
            return
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
            "source",
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
        optional_columns = {"source"}
        missing_required = allowed_columns - set(df.columns) - optional_columns
        if missing_required:
            raise RuntimeError(
                f"Snapshot {blob.name} is missing required columns: {sorted(missing_required)}"
            )
        for optional in optional_columns:
            if optional not in df.columns:
                df[optional] = ""
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
                job_id, job_type, miner_hotkey, source, payload,
                result_image_url, creation_timestamp, last_updated_at,
                miner_received_at, completed_at, execution_duration_ms,
                expires_at, status, audit_status, verification_status,
                is_audit_job, audit_target_job_id,
                prepared_at, dispatched_at, failure_reason,
                response_payload, response_timestamp,
                callback_secret, prompt_seed
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                parameters["job_id"],
                parameters["job_type"],
                parameters["miner_hotkey"],
                parameters.get("source", ""),
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
                       source,
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
            if row is not None:
                columns = [desc[0] for desc in cursor.description]
                return self._row_to_dict(columns, row)

        snapshot_records = self._load_latest_snapshot_records(job_id=str(job_id))
        if not snapshot_records:
            return None
        return snapshot_records[0]

    def fetch_all(self, limit: int | None = None) -> List[Dict[str, object]]:
        with self._read_cursor() as cursor:
            query = """
                SELECT job_id, job_type, miner_hotkey, payload,
                       source,
                       result_image_url, creation_timestamp, last_updated_at,
                       miner_received_at, completed_at, execution_duration_ms,
                       expires_at, status, audit_status, verification_status,
                       is_audit_job, audit_target_job_id,
                       prepared_at, dispatched_at, failure_reason,
                       response_payload, response_timestamp,
                       callback_secret, prompt_seed
                FROM inference_jobs
                ORDER BY COALESCE(completed_at, response_timestamp, last_updated_at, creation_timestamp) DESC
            """
            parameters: list[object] = []
            if limit is not None and limit > 0:
                query += " LIMIT ?"
                parameters.append(int(limit))
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            local_records = [self._row_to_dict(columns, row) for row in rows]

        snapshot_records = self._load_latest_snapshot_records()
        merged = self._merge_records(local_records, snapshot_records)
        sorted_records = self._sort_records(merged)
        if limit is not None and limit > 0:
            return sorted_records[:limit]
        return sorted_records

    def fetch_for_hotkey(
        self,
        miner_hotkey: str,
        *,
        since: datetime | None = None,
    ) -> List[Dict[str, object]]:
        with self._read_cursor() as cursor:
            clauses = ["miner_hotkey = ?"]
            parameters: list[object] = [miner_hotkey]
            if since is not None:
                cutoff = since
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=timezone.utc)
                else:
                    cutoff = cutoff.astimezone(timezone.utc)
                clauses.append("completed_at >= ?")
                parameters.append(cutoff)
            where_clause = " AND ".join(clauses)
            cursor.execute(
                f"""
                SELECT job_id, job_type, miner_hotkey, payload,
                       source,
                       result_image_url, creation_timestamp, last_updated_at,
                       miner_received_at, completed_at, execution_duration_ms,
                       expires_at, status, audit_status, verification_status,
                       is_audit_job, audit_target_job_id,
                       prepared_at, dispatched_at, failure_reason,
                       response_payload, response_timestamp,
                       callback_secret, prompt_seed
                FROM inference_jobs
                WHERE {where_clause}
                ORDER BY completed_at ASC, job_id
                """,
                parameters,
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            local_records = [self._row_to_dict(columns, row) for row in rows]

        snapshot_records = self._load_latest_snapshot_records(
            hotkey=miner_hotkey,
            since=since,
        )
        merged = self._merge_records(local_records, snapshot_records)
        return self._sort_records(merged)

    def delete_expired(self, now: datetime) -> int:
        result = self._write_conn.execute(
            "DELETE FROM inference_jobs WHERE expires_at <= ?", [now]
        )
        return result.fetchall()[0][0] if result.description else 0

    def purge_local(self, *, skip_snapshots: bool = False) -> dict[str, object]:
        """Clear local job state without touching remote snapshots."""

        deleted_jobs = 0
        try:
            result = self._write_conn.execute("SELECT COUNT(*) FROM inference_jobs").fetchone()
            if result is not None and result[0] is not None:
                deleted_jobs = int(result[0])
        except duckdb.Error:  # pragma: no cover - defensive
            LOGGER.warning("Failed counting inference jobs before purge", exc_info=True)

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
                LOGGER.debug("VACUUM unsupported or failed during purge", exc_info=True)
        except duckdb.Error as exc:  # pragma: no cover - severe failure path
            LOGGER.exception("Failed purging local DuckDB state: %s", exc)
            raise

        if skip_snapshots:
            self._skip_snapshot_io = True
            LOGGER.info("Snapshot IO disabled after purge; remote snapshots preserved")

        return {
            "deleted_jobs": deleted_jobs,
            "snapshots_disabled": self._skip_snapshot_io,
        }

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
        if self._skip_snapshot_io:
            LOGGER.info("Snapshot IO disabled; skipping sync to remote storage")
            return None
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

    def restore_all_snapshots(
        self,
        *,
        replace: bool = True,
        max_snapshots: int | None = None,
    ) -> dict[str, object]:
        """Load all GCS snapshots into local DuckDB without writing to GCS."""

        blobs = self._list_snapshot_blobs()
        if not blobs:
            LOGGER.info("No snapshot blobs found; skipping restore")
            return {"snapshots_processed": 0, "restored_rows": 0, "deduped_rows": 0}

        if max_snapshots is not None and max_snapshots > 0:
            blobs = blobs[-max_snapshots:]

        frames: list[pd.DataFrame] = []
        latest_snapshot_time: datetime | None = None

        for blob in blobs:
            snapshot_time = self._snapshot_time_from_blob(blob)
            latest_snapshot_time = max(latest_snapshot_time, snapshot_time) if latest_snapshot_time else snapshot_time
            local_path = None
            try:
                local_path = self._download_snapshot(blob)
                frame = pd.read_parquet(local_path, engine="pyarrow")
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.warning(
                    "snapshot.restore.read_failed blob=%s",
                    getattr(blob, "name", "<unknown>"),
                    exc_info=True,
                )
                continue
            finally:
                if local_path is not None:
                    try:
                        local_path.unlink(missing_ok=True)
                    except Exception:  # pragma: no cover - best effort
                        LOGGER.debug("snapshot.restore.temp_cleanup_failed", exc_info=True)

            if frame.empty:
                continue

            frame = self._normalize_snapshot_dataframe(frame)
            frame["snapshot_epoch"] = snapshot_time
            frames.append(frame)

        if not frames:
            LOGGER.info("No rows found across snapshot blobs; skipping restore")
            return {"snapshots_processed": 0, "restored_rows": 0, "deduped_rows": 0}

        combined = pd.concat(frames, ignore_index=True)
        combined["job_id"] = combined["job_id"].astype(str)

        deduped = combined.sort_values(
            by=["snapshot_epoch", "completed_at", "job_id"],
            kind="stable",
        ).drop_duplicates(subset=["job_id"], keep="last")

        records = deduped[self._FETCH_COLUMNS].to_dict(orient="records")
        if replace:
            self._write_conn.execute("DELETE FROM inference_jobs")
        self._write_conn.register("snapshot_restore_all", deduped[self._FETCH_COLUMNS])
        columns_csv = ", ".join(self._FETCH_COLUMNS)
        self._write_conn.execute(
            f"INSERT INTO inference_jobs ({columns_csv}) SELECT {columns_csv} FROM snapshot_restore_all"
        )
        self._write_conn.unregister("snapshot_restore_all")

        if latest_snapshot_time is not None:
            self._set_last_snapshot_time(latest_snapshot_time)

        LOGGER.info(
            "Restored %s inference job records from %s snapshots",
            len(deduped.index),
            len(frames),
        )
        return {
            "snapshots_processed": len(frames),
            "restored_rows": len(deduped.index),
            "deduped_rows": len(deduped.index),
        }

    def _merge_records(
        self,
        local_records: List[Dict[str, object]],
        snapshot_records: List[Dict[str, object]],
    ) -> List[Dict[str, object]]:
        if not snapshot_records:
            return local_records
        merged: Dict[str, Dict[str, object]] = {
            str(record.get("job_id")): record for record in local_records
        }
        for record in snapshot_records:
            job_id = str(record.get("job_id"))
            if job_id in merged:
                continue
            merged[job_id] = record
        return list(merged.values())

    def _sort_records(self, records: List[Dict[str, object]]) -> List[Dict[str, object]]:
        def _parse_completed(value: Any) -> datetime:
            if isinstance(value, datetime):
                return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            if hasattr(value, "to_pydatetime"):
                candidate = value.to_pydatetime()
                return candidate if candidate.tzinfo else candidate.replace(tzinfo=timezone.utc)
            if isinstance(value, str):
                try:
                    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                except ValueError:
                    return datetime.min.replace(tzinfo=timezone.utc)
            return datetime.min.replace(tzinfo=timezone.utc)

        return sorted(
            records,
            key=lambda record: (
                _parse_completed(record.get("completed_at")),
                str(record.get("job_id")),
            ),
        )

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
        candidates = self._list_snapshot_blobs()
        if not candidates:
            return None
        latest = candidates[-1]
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

    def _load_latest_snapshot_records(
        self,
        *,
        hotkey: str | None = None,
        since: datetime | None = None,
        job_id: str | None = None,
    ) -> List[Dict[str, object]]:
        if self._skip_snapshot_io:
            return []
        bucket = self._settings.gcs_bucket
        if not bucket:
            return []

        try:
            snapshot = self._fetch_latest_snapshot_metadata()
        except FileNotFoundError:
            return []
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning(
                "snapshot.list_failed bucket=%s prefix=%s",
                bucket,
                self._settings.gcs_prefix,
                exc_info=exc,
            )
            return []

        if snapshot is None:
            return []

        blob, _snapshot_time = snapshot
        local_path: Path | None = None
        try:
            local_path = self._download_snapshot(blob)
            df = pd.read_parquet(local_path, engine="pyarrow")
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning(
                "snapshot.read_failed blob=%s",
                getattr(blob, "name", "<unknown>"),
                exc_info=exc,
            )
            return []
        finally:
            if local_path is not None:
                try:
                    local_path.unlink(missing_ok=True)
                except Exception:  # pragma: no cover - best effort cleanup
                    LOGGER.debug("snapshot.temp_cleanup_failed", exc_info=True)

        if df.empty:
            return []

        normalized = self._normalize_snapshot_dataframe(df)
        if hotkey:
            normalized = normalized[normalized["miner_hotkey"] == hotkey]
        if job_id:
            normalized = normalized[normalized["job_id"] == str(job_id)]
        if since is not None and "completed_at" in normalized.columns:
            cutoff = since
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)
            else:
                cutoff = cutoff.astimezone(timezone.utc)
            normalized = normalized[
                normalized["completed_at"].notna() & (normalized["completed_at"] >= cutoff)
            ]

        if normalized.empty:
            return []

        records = normalized[self._FETCH_COLUMNS].to_dict(orient="records")
        return [self._coerce_snapshot_record(record) for record in records]

    def _normalize_snapshot_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        frame = df.copy()
        for column in self._FETCH_COLUMNS:
            if column not in frame.columns:
                frame[column] = None

        time_columns = [
            "creation_timestamp",
            "last_updated_at",
            "miner_received_at",
            "completed_at",
            "expires_at",
            "prepared_at",
            "dispatched_at",
            "response_timestamp",
            "snapshot_epoch",
        ]
        for column in time_columns:
            if column in frame.columns:
                frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")

        frame["job_id"] = frame["job_id"].astype(str)
        if "audit_target_job_id" in frame.columns:
            frame["audit_target_job_id"] = frame["audit_target_job_id"].astype("string")

        missing_payload = [col for col in ("payload", "response_payload") if col not in frame.columns]
        for column in missing_payload:
            frame[column] = None

        return frame

    def _coerce_snapshot_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        parsed = dict(record)

        def _clean_value(value: Any) -> Any:
            # Treat pandas NA/NaT as missing to avoid serialization errors downstream.
            try:
                import pandas as pd  # type: ignore
            except Exception:  # pragma: no cover - pandas available in runtime image
                pd = None
            if pd is not None and pd.isna(value):
                return None
            if pd is not None and isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            return value

        job_id = parsed.get("job_id")
        if job_id is not None:
            parsed["job_id"] = str(job_id)

        payload = parsed.get("payload")
        if isinstance(payload, str):
            try:
                parsed["payload"] = json.loads(payload)
            except Exception:
                LOGGER.debug("snapshot.payload_parse_failed", exc_info=True)

        response_payload = parsed.get("response_payload")
        if isinstance(response_payload, str):
            try:
                parsed["response_payload"] = json.loads(response_payload)
            except Exception:
                LOGGER.debug("snapshot.response_payload_parse_failed", exc_info=True)

        # Normalize any NaN/NaT sentinel values to None for safe Pydantic serialization.
        for key, value in list(parsed.items()):
            parsed[key] = _clean_value(value)

        return parsed

    def _list_snapshot_blobs(self) -> list:
        client = self._build_storage_client()
        prefix = (self._settings.gcs_prefix or "").strip("/")
        prefix_arg = f"{prefix}/" if prefix else None
        blobs = [
            blob
            for blob in client.list_blobs(self._settings.gcs_bucket, prefix=prefix_arg)
            if hasattr(blob, "name") and blob.name.endswith(".parquet")
        ]
        blobs.sort(key=self._snapshot_time_from_blob)
        return blobs
