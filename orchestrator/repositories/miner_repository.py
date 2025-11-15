from __future__ import annotations

import json
import logging
from typing import Dict, Iterator, Mapping, Optional, Tuple

from psycopg.types.json import Json

from orchestrator.clients.database import PostgresClient
from orchestrator.common.model_utils import dump_model, validate_model
from orchestrator.domain.miner import Miner


class MinerRepository:
    """Persistence layer for the miners table."""

    def __init__(self, database_service: PostgresClient) -> None:
        self._database_service = database_service
        self._logger = logging.getLogger(__name__)
        self._ensure_schema()

    def list_hotkeys(self) -> set[str]:
        with self._database_service.cursor() as cur:
            cur.execute("SELECT hotkey FROM miners")
            return {row[0] for row in cur.fetchall()}

    def sync_state(self, state: Dict[str, Miner]) -> None:
        new_keys = set(state.keys())
        existing_keys = self.list_hotkeys()
        to_delete = existing_keys - new_keys

        with self._database_service.cursor() as cur:
            if to_delete:
                cur.executemany(
                    "DELETE FROM miners WHERE hotkey = %s",
                    [(key,) for key in to_delete],
                )

            rows = [
                (
                    hotkey,
                    Json(self._miner_to_payload(self._coerce_miner(miner))),
                )
                for hotkey, miner in state.items()
            ]
            if rows:
                cur.executemany(
                    """
                    INSERT INTO miners (hotkey, value)
                    VALUES (%s, %s)
                    ON CONFLICT(hotkey) DO UPDATE SET value = EXCLUDED.value
                    """,
                    rows,
                )

    def upsert_miner(self, payload: Miner | Mapping[str, object]) -> Miner:
        model = self._coerce_miner(payload)
        if not model.hotkey:
            raise ValueError("Miner record must include a hotkey")

        with self._database_service.cursor() as cur:
            cur.execute(
                """
                INSERT INTO miners (hotkey, value)
                VALUES (%s, %s)
                ON CONFLICT(hotkey) DO UPDATE SET value = EXCLUDED.value
                RETURNING value, scores
                """,
                (
                    model.hotkey,
                    Json(self._miner_to_payload(model)),
                ),
            )
            stored = cur.fetchone()
        if not stored:
            return model
        miner, _ = self._miner_from_payloads(*stored)
        return miner

    def get_miner(self, hotkey: str) -> Optional[Miner]:
        with self._database_service.cursor() as cur:
            cur.execute(
                "SELECT value, scores FROM miners WHERE hotkey = %s",
                (hotkey,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        miner, _ = self._miner_from_payloads(*row)
        return miner

    def delete_miner(self, hotkey: str) -> bool:
        with self._database_service.cursor() as cur:
            cur.execute(
                "DELETE FROM miners WHERE hotkey = %s",
                (hotkey,),
            )
            deleted = cur.rowcount > 0
        return deleted

    def iter_miners(self, *, valid_only: bool = False) -> Iterator[tuple[str, Miner]]:
        query = "SELECT hotkey, value, scores FROM miners"
        if valid_only:
            query += " WHERE (value->>'valid')::boolean IS TRUE"

        with self._database_service.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        for hotkey, value_payload, score_payload in rows:
            try:
                miner, _ = self._miner_from_payloads(value_payload, score_payload)
                yield hotkey, miner
            except Exception as exc:  # pragma: no cover - defensive guard
                self._logger.debug(
                    "miner_repository.iter_miners.skip hotkey=%s error=%s",
                    hotkey,
                    exc,
                )

    def dump_state(self) -> Dict[str, Miner]:
        return {hotkey: miner for hotkey, miner in self.iter_miners()}

    def dump_filtered_state(self) -> Dict[str, Miner]:
        return {hotkey: miner for hotkey, miner in self.iter_miners(valid_only=True)}

    def fetch_miners(self, *, valid_only: bool = True) -> Dict[str, Miner]:
        return {hotkey: miner for hotkey, miner in self.iter_miners(valid_only=valid_only)}

    def fetch_candidate_records(
        self,
        *,
        task_type: str | None = None,
    ) -> list[tuple[Miner, Optional[Mapping[str, object]]]]:
        query = [
            "SELECT value, scores FROM miners",
            "WHERE (value->>'valid')::boolean IS TRUE",
        ]
        params: list[object] = []
        if task_type:
            query.append(
                "AND ("
                "COALESCE(value->'capacity', '{}'::jsonb) @> jsonb_build_object(%s::text, true) "
                "OR EXISTS ("
                "SELECT 1 FROM jsonb_array_elements_text("
                "COALESCE(value->'capacity'->'inference', '[]'::jsonb)"
                ") AS inference(capability)"
                " WHERE inference.capability = %s"
                ")"
                ")"
            )
            params.extend((task_type, task_type))

        with self._database_service.cursor() as cur:
            cur.execute(" ".join(query), tuple(params))
            rows = cur.fetchall()

        records: list[tuple[Miner, Optional[Mapping[str, object]]]] = []
        for value_payload, score_payload in rows:
            try:
                record = self._miner_from_payloads(value_payload, score_payload)
            except Exception as exc:  # pragma: no cover - defensive guard
                self._logger.debug(
                    "miner_repository.fetch_candidate_records.skip error=%s",
                    exc,
                )
                continue
            records.append(record)
        return records

    def _miner_to_payload(self, miner: Miner) -> dict[str, object]:
        payload = dump_model(miner)
        payload.pop("failure_count", None)
        return payload

    def _payload_to_miner(self, payload: object) -> Miner:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            payload = payload.decode()
        if isinstance(payload, str):
            data = json.loads(payload)
        else:
            data = payload
        return validate_model(Miner, data)

    def _miner_from_payloads(
        self,
        value_payload: object,
        score_payload: object | None,
    ) -> tuple[Miner, Optional[Mapping[str, object]]]:
        miner = self._payload_to_miner(value_payload)
        scores_mapping = self._coerce_scores_payload(score_payload)
        failure_count = self._failure_count_from_scores(scores_mapping)
        try:
            miner.failure_count = failure_count
        except (TypeError, ValueError, AttributeError):
            payload = dump_model(miner)
            payload["failure_count"] = failure_count
            miner = self._coerce_miner(payload)
        return miner, scores_mapping

    def _coerce_miner(self, payload: Miner | Mapping[str, object]) -> Miner:
        if isinstance(payload, Miner):
            return payload
        return validate_model(Miner, dict(payload))

    def _coerce_scores_payload(self, payload: object) -> Optional[Mapping[str, object]]:
        if payload in (None, {}, ()):  # fast path for empty values
            return None
        if isinstance(payload, Mapping):
            return dict(payload)
        if isinstance(payload, (bytes, bytearray, memoryview)):
            payload = payload.decode()
        if isinstance(payload, str):
            try:
                data = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                return None
            if isinstance(data, Mapping):
                return dict(data)
            return None
        return None

    def _failure_count_from_scores(self, payload: Optional[Mapping[str, object]]) -> int:
        if not payload:
            return 0
        candidate = payload.get("failure_count")
        try:
            value = int(candidate)
        except (TypeError, ValueError):
            return 0
        return max(value, 0)

    def _ensure_schema(self) -> None:
        with self._database_service.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS miners (
                    hotkey TEXT PRIMARY KEY,
                    value  JSONB NOT NULL,
                    scores JSONB
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE miners
                ADD COLUMN IF NOT EXISTS scores JSONB
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_miners_valid
                ON miners ((value->>'valid'))
                """
            )


__all__ = ["MinerRepository"]
