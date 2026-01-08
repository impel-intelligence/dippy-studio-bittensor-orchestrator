from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from psycopg.types.json import Json

from orchestrator.clients.database import PostgresClient


@dataclass
class AuditRecord:
    hotkey: str
    failed_job: Mapping[str, Any]
    reference_job: Mapping[str, Any]


class AuditRepository:
    """Append-only storage for audit reference and failed job pairs."""

    def __init__(self, database_service: PostgresClient) -> None:
        self._database_service = database_service
        self._logger = logging.getLogger(__name__)
        self._ensure_schema()

    def insert(self, record: AuditRecord) -> None:
        hotkey = (record.hotkey or "").strip()
        if not hotkey:
            raise ValueError("Audit hotkey is required")

        failed_job = dict(record.failed_job)
        reference_job = dict(record.reference_job)

        with self._database_service.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audits (hotkey, failed_job, reference_job)
                VALUES (%s, %s, %s)
                ON CONFLICT (hotkey) DO NOTHING
                """,
                (hotkey, Json(failed_job), Json(reference_job)),
            )

    def fetch(self, hotkey: str) -> AuditRecord | None:
        """Return the audit record for a given miner hotkey."""

        candidate = (hotkey or "").strip()
        if not candidate:
            return None

        with self._database_service.cursor() as cur:
            cur.execute(
                """
                SELECT hotkey, failed_job, reference_job
                FROM audits
                WHERE hotkey = %s
                LIMIT 1
                """,
                (candidate,),
            )
            row = cur.fetchone()

        if not row:
            return None

        if isinstance(row, Mapping):
            stored_hotkey = row.get("hotkey", candidate)
            failed_job = row.get("failed_job")
            reference_job = row.get("reference_job")
        else:
            try:
                stored_hotkey, failed_job, reference_job = row
            except Exception:
                # Fallback to attribute-style access for defensive compatibility
                stored_hotkey = getattr(row, "hotkey", candidate)
                failed_job = getattr(row, "failed_job", None)
                reference_job = getattr(row, "reference_job", None)

        return AuditRecord(
            hotkey=stored_hotkey or candidate,
            failed_job=failed_job,
            reference_job=reference_job,
        )

    def _ensure_schema(self) -> None:
        """Create the audits table optimized for append-only inserts."""
        with self._database_service.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS audits (
                    hotkey TEXT PRIMARY KEY,
                    failed_job JSONB NOT NULL,
                    reference_job JSONB NOT NULL
                )
                WITH (
                    fillfactor = 100,
                    autovacuum_vacuum_insert_scale_factor = 0.2,
                    autovacuum_analyze_scale_factor = 0.02
                )
                """
            )


__all__ = ["AuditRecord", "AuditRepository"]
