from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from psycopg.types.json import Json

from orchestrator.clients.database import PostgresClient


@dataclass
class AuditFailureRecord:
    id: uuid.UUID
    audit_job_id: uuid.UUID
    target_job_id: uuid.UUID | None
    miner_hotkey: str | None
    netuid: int | None
    network: str | None
    audit_payload: Mapping[str, Any] | None
    audit_response_payload: Mapping[str, Any] | None
    target_payload: Mapping[str, Any] | None
    target_response_payload: Mapping[str, Any] | None
    audit_image_hash: str | None
    target_image_hash: str | None
    created_at: datetime


class AuditFailureRepository:
    """Persistence layer for audit failure records."""

    def __init__(self, database_service: PostgresClient) -> None:
        self._database_service = database_service
        self._logger = logging.getLogger(__name__)
        self._ensure_schema()

    def upsert_failure(self, record: AuditFailureRecord) -> None:
        created_at = self._coerce_timestamp(record.created_at)
        payload = (
            record.id,
            created_at,
            record.audit_job_id,
            record.target_job_id,
            record.miner_hotkey,
            record.netuid,
            record.network,
            Json(record.audit_payload) if record.audit_payload is not None else None,
            Json(record.audit_response_payload) if record.audit_response_payload is not None else None,
            Json(record.target_payload) if record.target_payload is not None else None,
            Json(record.target_response_payload) if record.target_response_payload is not None else None,
            record.audit_image_hash,
            record.target_image_hash,
        )

        with self._database_service.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_failures (
                    id,
                    created_at,
                    audit_job_id,
                    target_job_id,
                    miner_hotkey,
                    netuid,
                    network,
                    audit_payload,
                    audit_response_payload,
                    target_payload,
                    target_response_payload,
                    audit_image_hash,
                    target_image_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (audit_job_id) DO UPDATE SET
                    target_job_id = EXCLUDED.target_job_id,
                    miner_hotkey = EXCLUDED.miner_hotkey,
                    netuid = EXCLUDED.netuid,
                    network = EXCLUDED.network,
                    audit_payload = EXCLUDED.audit_payload,
                    audit_response_payload = EXCLUDED.audit_response_payload,
                    target_payload = EXCLUDED.target_payload,
                    target_response_payload = EXCLUDED.target_response_payload,
                    audit_image_hash = EXCLUDED.audit_image_hash,
                    target_image_hash = EXCLUDED.target_image_hash,
                    created_at = audit_failures.created_at
                """,
                payload,
            )

    def list_recent(self, *, limit: int = 100) -> list[AuditFailureRecord]:
        fetch_limit = max(1, int(limit))
        with self._database_service.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    created_at,
                    audit_job_id,
                    target_job_id,
                    miner_hotkey,
                    netuid,
                    network,
                    audit_payload,
                    audit_response_payload,
                    target_payload,
                    target_response_payload,
                    audit_image_hash,
                    target_image_hash
                FROM audit_failures
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (fetch_limit,),
            )
            rows = cur.fetchall()

        return [self._row_to_record(row) for row in rows]

    def _coerce_timestamp(self, value: datetime | None) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _row_to_record(self, row: Sequence[Any]) -> AuditFailureRecord:
        (
            record_id,
            created_at,
            audit_job_id,
            target_job_id,
            miner_hotkey,
            netuid,
            network,
            audit_payload,
            audit_response_payload,
            target_payload,
            target_response_payload,
            audit_image_hash,
            target_image_hash,
        ) = row
        return AuditFailureRecord(
            id=record_id,
            created_at=self._coerce_timestamp(created_at),
            audit_job_id=audit_job_id,
            target_job_id=target_job_id,
            miner_hotkey=miner_hotkey,
            netuid=netuid,
            network=network,
            audit_payload=audit_payload,
            audit_response_payload=audit_response_payload,
            target_payload=target_payload,
            target_response_payload=target_response_payload,
            audit_image_hash=audit_image_hash,
            target_image_hash=target_image_hash,
        )

    def _ensure_schema(self) -> None:
        with self._database_service.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_failures (
                    id UUID PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    audit_job_id UUID NOT NULL UNIQUE,
                    target_job_id UUID,
                    miner_hotkey TEXT,
                    netuid INTEGER,
                    network TEXT,
                    audit_payload JSONB,
                    audit_response_payload JSONB,
                    target_payload JSONB,
                    target_response_payload JSONB,
                    audit_image_hash TEXT,
                    target_image_hash TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_failures_created_at
                ON audit_failures (created_at)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_failures_hotkey
                ON audit_failures (miner_hotkey)
                """
            )


__all__ = ["AuditFailureRepository", "AuditFailureRecord"]
