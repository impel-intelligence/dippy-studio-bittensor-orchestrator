"""Postgres-backed storage manager for JobRelay."""

from __future__ import annotations

import logging
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID

import pandas as pd
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - fail fast when dependency missing
    raise ImportError("pyarrow is required for JobRelay snapshots") from exc

from .config import Settings

LOGGER = logging.getLogger("jobrelay.postgres_manager")


class JobRelayPostgresManager:
    """Owns Postgres connections, schema, and snapshot plumbing."""

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
    ]

    def __init__(self, settings: Settings):
        self.supports_snapshot_loop = False
        if not settings.postgres_dsn:
            raise ValueError("JOBRELAY_POSTGRES_DSN must be configured for Postgres storage")
        self._settings = settings
        self._skip_snapshot_io = False
        self._pool = ConnectionPool(
            settings.postgres_dsn,
            min_size=settings.postgres_min_connections,
            max_size=settings.postgres_max_connections,
            kwargs={"autocommit": False},
        )
        self._pool.wait()
        self._ensure_schema()

    def insert_job(self, record: Dict[str, object]) -> None:
        columns = [
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
        ]
        values = [record.get(col) for col in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        query = sql.SQL(
            f"INSERT INTO inference_jobs ({', '.join(columns)}) VALUES ({placeholders})"
        )
        self._execute(query, values)

    def update_job(self, job_id: UUID, updates: Dict[str, object]) -> None:
        if not updates:
            return
        assignments: list[str] = []
        parameters: list[object] = []
        for key, value in updates.items():
            assignments.append(f"{key} = %s")
            parameters.append(value)
        parameters.append(job_id)
        query = sql.SQL(f"UPDATE inference_jobs SET {', '.join(assignments)} WHERE job_id = %s")
        self._execute(query, parameters)

    def fetch_job(self, job_id: UUID) -> Optional[Dict[str, object]]:
        query = sql.SQL(
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
            WHERE job_id = %s
            """
        )
        rows = self._query(query, [job_id])
        if rows:
            return rows[0]
        LOGGER.info("Postgres fetch_job miss; snapshot fallback not supported in Postgres manager")
        return None

    def fetch_all(self, limit: int | None = None) -> List[Dict[str, object]]:
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
        params: list[object] = []
        if limit is not None and limit > 0:
            query += " LIMIT %s"
            params.append(int(limit))
        local_records = self._query(query, params)
        snapshot_records = self._load_latest_snapshot_records()
        merged = self._merge_records(local_records, snapshot_records)
        sorted_records = self._sort_records(merged)
        if limit is not None and limit > 0:
            return local_records[:limit]
        return local_records

    def fetch_for_hotkey(
        self,
        miner_hotkey: str,
        *,
        since: datetime | None = None,
    ) -> List[Dict[str, object]]:
        clauses = ["miner_hotkey = %s"]
        params: list[object] = [miner_hotkey]
        if since is not None:
            cutoff = since
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)
            else:
                cutoff = cutoff.astimezone(timezone.utc)
            clauses.append("completed_at >= %s")
            params.append(cutoff)
        where_clause = " AND ".join(clauses)
        query = f"""
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
        """
        return self._query(query, params)

    def delete_expired(self, now: datetime) -> int:
        query = "DELETE FROM inference_jobs WHERE expires_at <= %s"
        return self._execute(query, [now])

    def purge_local(self, *, skip_snapshots: bool = False) -> dict[str, object]:
        deleted_jobs = self._execute("DELETE FROM inference_jobs")
        self._execute("DELETE FROM duckdb_sync_state")
        self._execute(
            """
            INSERT INTO duckdb_sync_state (id, last_snapshot)
            VALUES (1, TIMESTAMPTZ '1970-01-01 00:00:00+00')
            ON CONFLICT (id) DO UPDATE SET last_snapshot = EXCLUDED.last_snapshot
            """
        )
        if skip_snapshots:
            self._skip_snapshot_io = True
            LOGGER.info("Snapshot IO disabled after purge; remote snapshots preserved")
        return {
            "deleted_jobs": deleted_jobs,
            "snapshots_disabled": self._skip_snapshot_io,
        }

    def checkpoint(self) -> None:
        # Postgres fsync is managed by the database; nothing to do here.
        return None

    def close(self) -> None:
        self._pool.close()

    def nuclear_wipe(self) -> dict[str, int]:
        deleted_jobs = self._execute("DELETE FROM inference_jobs")
        self._execute("DELETE FROM duckdb_sync_state")
        self._execute(
            """
            INSERT INTO duckdb_sync_state (id, last_snapshot)
            VALUES (1, TIMESTAMPTZ '1970-01-01 00:00:00+00')
            ON CONFLICT (id) DO UPDATE SET last_snapshot = EXCLUDED.last_snapshot
            """
        )
        snapshots_deleted = self._delete_all_snapshots()
        return {
            "deleted_jobs": deleted_jobs,
            "snapshots_deleted": snapshots_deleted,
        }

    def sync_to_gcs(self, snapshot_time: Optional[datetime] = None) -> Optional[str]:
        LOGGER.info("Postgres manager: sync_to_gcs is not supported; use flush instead")
        return None

    def flush(self, snapshot_time: Optional[datetime] = None) -> Optional[str]:
        """Export non-pending jobs to Parquet in object storage and delete them from Postgres."""

        snapshot_time = snapshot_time or datetime.now(timezone.utc)
        query = """
            SELECT * FROM inference_jobs
            WHERE status <> 'pending'
        """
        with self._pool.connection() as conn:
            frame = pd.read_sql_query(query, conn)

        if frame.empty:
            LOGGER.info("No completed/failed jobs to flush; skipping upload")
            return None

        batch = frame.copy()
        batch["job_id"] = batch["job_id"].apply(str)
        if "audit_target_job_id" in batch.columns:
            batch["audit_target_job_id"] = batch["audit_target_job_id"].apply(
                lambda value: str(value) if value is not None and not pd.isna(value) else None
            )
        batch["snapshot_epoch"] = snapshot_time

        local_path = self._write_snapshot_to_parquet(batch)
        try:
            gcs_uri = self._upload_snapshot(local_path, snapshot_time)
        finally:
            try:
                local_path.unlink(missing_ok=True)
            except OSError:  # pragma: no cover - best effort cleanup
                LOGGER.debug("Temporary snapshot file cleanup failed", exc_info=True)

        # Delete flushed rows
        ids = tuple(batch["job_id"].tolist())
        delete_query = sql.SQL("DELETE FROM inference_jobs WHERE job_id = ANY(%s)")
        deleted = self._execute(delete_query, [list(ids)])
        if deleted != len(batch.index):
            LOGGER.warning(
                "Flushed delete count mismatch; attempted=%s deleted=%s", len(batch.index), deleted
            )
        LOGGER.info("Flushed %s jobs to %s and removed them from Postgres", len(batch.index), gcs_uri)
        return gcs_uri

    def _query(self, query: str, params: Optional[List[object]] = None) -> List[Dict[str, object]]:
        params = params or []
        with self._pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _execute(self, query: str | sql.SQL, params: Optional[List[object]] = None) -> int:
        params = params or []
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rowcount = cur.rowcount or 0
            conn.commit()
        return rowcount

    def _row_to_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        as_dict = dict(row)
        return as_dict

    def _get_last_snapshot_time(self) -> Optional[datetime]:
        rows = self._query("SELECT last_snapshot FROM duckdb_sync_state WHERE id = 1")
        if not rows:
            return None
        return rows[0].get("last_snapshot")

    def _set_last_snapshot_time(self, timestamp: datetime) -> None:
        self._execute(
            """
            INSERT INTO duckdb_sync_state (id, last_snapshot)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET last_snapshot = EXCLUDED.last_snapshot
            """,
            [timestamp],
        )

    def _ensure_schema(self) -> None:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS inference_jobs (
                        job_id UUID PRIMARY KEY,
                        job_type TEXT NOT NULL,
                        miner_hotkey TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT '',
                        payload JSONB NOT NULL,
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
                        response_payload JSONB,
                        response_timestamp TIMESTAMPTZ,
                        callback_secret TEXT,
                        prompt_seed BIGINT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS duckdb_sync_state (
                        id INTEGER PRIMARY KEY,
                        last_snapshot TIMESTAMPTZ
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO duckdb_sync_state (id, last_snapshot)
                    VALUES (1, TIMESTAMPTZ '1970-01-01 00:00:00+00')
                    ON CONFLICT (id) DO NOTHING
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_inference_jobs_miner_hotkey
                    ON inference_jobs(miner_hotkey)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_inference_jobs_hotkey_completed
                    ON inference_jobs(miner_hotkey, completed_at)
                    """
                )
            conn.commit()

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

    def _snapshot_time_from_blob(self, blob) -> datetime:
        timestamp = getattr(blob, "time_created", None)
        if timestamp is not None:
            return timestamp.astimezone(timezone.utc)
        match = re.search(r"snapshot_(\d{8}_\d{6})", getattr(blob, "name", ""))
        if match:
            parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
            return parsed.replace(tzinfo=timezone.utc)
        raise FileNotFoundError("Unable to determine snapshot timestamp from blob metadata")

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

    def _download_snapshot(self, blob) -> Path:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            blob.download_to_filename(tmp.name)
            return Path(tmp.name)

    def _fetch_latest_snapshot_metadata(self):
        candidates = self._list_snapshot_blobs()
        if not candidates:
            return None
        latest = candidates[-1]
        snapshot_time = self._snapshot_time_from_blob(latest)
        return latest, snapshot_time

    def _load_latest_snapshot_records(
        self,
        *,
        hotkey: str | None = None,
        since: datetime | None = None,
        job_id: str | None = None,
    ) -> List[Dict[str, object]]:
        LOGGER.info("Postgres manager: snapshot fallback is not supported")
        return []

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

        for key, value in list(parsed.items()):
            parsed[key] = _clean_value(value)

        return parsed

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
