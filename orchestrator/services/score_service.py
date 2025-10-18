from __future__ import annotations

import json
import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

try:
    from pydantic import BaseModel, ConfigDict
except ImportError:
    from pydantic import BaseModel  # type: ignore

    ConfigDict = None  # type: ignore

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.clients.miner_metagraph import Miner, LiveMinerMetagraphClient
from orchestrator.schemas.scores import ScorePayload, ScoreValue, ScoresResponse
from orchestrator.services.database_service import DatabaseService
from orchestrator.services.job_scoring import job_to_score

if TYPE_CHECKING:
    from orchestrator.services.job_service import JobService
    from orchestrator.services.subnet_state_service import SubnetStateService


logger = logging.getLogger(__name__)


class ScoreRecord(BaseModel):
    scores: float
    is_slashed: bool

    if ConfigDict is not None:  # type: ignore[truthy-bool]
        model_config = ConfigDict(extra="allow")  # type: ignore[assignment]
    else:
        class Config:
            extra = "allow"


class ScoreService:

    _TABLE_NAME = "scores"

    _COMPLETED_STATUSES = {"success", "failed", "timeout"}

    def __init__(
        self,
        database_service: DatabaseService,
        *,
        job_service: "JobService | None" = None,
        subnet_state_service: "SubnetStateService | None" = None,
        miner_metagraph_client: LiveMinerMetagraphClient | None = None,
        netuid: int | None = None,
        network: str | None = None,
        fetch_concurrency: int = 8,
    ) -> None:
        self._database_service = database_service
        self._conn = self._database_service.get_connection()
        self._db_path = str(self._database_service.path)
        self._ensure_schema()
        self._last_update: Optional[datetime] = None
        self._job_service = job_service
        self._subnet_state_service = subnet_state_service
        self._netuid = netuid
        self._network = network
        self._fetch_concurrency = max(1, int(fetch_concurrency)) if fetch_concurrency else 1
        self._miner_metagraph_client = miner_metagraph_client

    @property
    def db_path(self) -> str:
        return self._db_path

    def last_update(self) -> Optional[datetime]:
        return self._last_update

    def put(self, hotkey: str, record: Mapping[str, Any] | ScoreRecord) -> None:
        self.put_many({hotkey: record})

    def put_many(self, records: Mapping[str, Mapping[str, Any] | ScoreRecord]) -> None:
        if not records:
            return

        prepared = [self._prepare_row(hotkey, value) for hotkey, value in records.items()]
        cur = self._conn.cursor()
        cur.executemany(
            f"""
            INSERT INTO {self._TABLE_NAME} (hotkey, value)
            VALUES (?, ?)
            ON CONFLICT(hotkey) DO UPDATE SET value=excluded.value
            """,
            prepared,
        )
        self._conn.commit()
        self._last_update = datetime.now(timezone.utc)

    def replace_all(self, records: Mapping[str, Mapping[str, Any] | ScoreRecord]) -> None:
        """Replace the entire score set with the provided records."""
        cur = self._conn.cursor()
        cur.execute(f"DELETE FROM {self._TABLE_NAME}")
        self._conn.commit()
        if records:
            self.put_many(records)
        else:
            self._last_update = datetime.now(timezone.utc)

    def delete(self, hotkey: str) -> None:
        cur = self._conn.cursor()
        cur.execute(
            f"DELETE FROM {self._TABLE_NAME} WHERE hotkey = ?",
            (hotkey,),
        )
        self._conn.commit()
        self._last_update = datetime.now(timezone.utc)

    def get(self, hotkey: str) -> Optional[ScoreRecord]:
        cur = self._conn.cursor()
        cur.execute(
            f"SELECT value FROM {self._TABLE_NAME} WHERE hotkey = ?",
            (hotkey,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return self._json_to_record(row[0])

    def all(self) -> Dict[str, ScoreRecord]:
        cur = self._conn.cursor()
        cur.execute(f"SELECT hotkey, value FROM {self._TABLE_NAME}")
        return {hotkey: self._json_to_record(payload) for hotkey, payload in cur.fetchall()}

    def items(self) -> Iterable[Tuple[str, ScoreRecord]]:
        cur = self._conn.cursor()
        cur.execute(f"SELECT hotkey, value FROM {self._TABLE_NAME}")
        for hotkey, payload in cur.fetchall():
            yield hotkey, self._json_to_record(payload)

    def clear(self) -> None:
        cur = self._conn.cursor()
        cur.execute(f"DELETE FROM {self._TABLE_NAME}")
        self._conn.commit()
        self._last_update = datetime.now(timezone.utc)

    def _ensure_schema(self) -> None:
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._TABLE_NAME} (
                hotkey TEXT PRIMARY KEY,
                value  TEXT NOT NULL CHECK (json_valid(value))
            ) STRICT
            """
        )
        self._conn.commit()

    def _prepare_row(self, hotkey: str, value: Mapping[str, Any] | ScoreRecord) -> Tuple[str, str]:
        record = self._coerce_record(value)
        return hotkey, self._record_to_json(record)

    def _coerce_record(self, value: Mapping[str, Any] | ScoreRecord) -> ScoreRecord:
        if isinstance(value, ScoreRecord):
            return value

        if hasattr(value, "model_dump"):
            data = value.model_dump()  # type: ignore[attr-defined]
        else:
            data = dict(value)

        if "scores" not in data:
            raise ValueError("score payload must include a 'scores' field")
        if "is_slashed" not in data:
            raise ValueError("score payload must include an 'is_slashed' field")

        data = {
            **data,
            "scores": float(data["scores"]),
            "is_slashed": bool(data["is_slashed"]),
        }

        if hasattr(ScoreRecord, "model_validate"):
            return ScoreRecord.model_validate(data)  # type: ignore[attr-defined]
        return ScoreRecord(**data)

    def _record_to_json(self, record: ScoreRecord) -> str:
        if hasattr(record, "model_dump_json"):
            return record.model_dump_json()
        if hasattr(record, "json"):
            return record.json()
        return json.dumps(record.model_dump())

    def _json_to_record(self, payload: str) -> ScoreRecord:
        if hasattr(ScoreRecord, "model_validate_json"):
            return ScoreRecord.model_validate_json(payload)  # type: ignore[attr-defined]
        if hasattr(ScoreRecord, "parse_raw"):
            return ScoreRecord.parse_raw(payload)  # type: ignore[attr-defined]
        return ScoreRecord(**json.loads(payload))

    def set_job_service(self, job_service: "JobService | None") -> None:

        self._job_service = job_service

    def set_subnet_state_service(
        self,
        subnet_state_service: "SubnetStateService | None",
        *,
        netuid: int | None = None,
        network: str | None = None,
    ) -> None:

        self._subnet_state_service = subnet_state_service
        if netuid is not None:
            self._netuid = netuid
        if network is not None:
            self._network = network

    def set_miner_metagraph_client(
        self,
        client: LiveMinerMetagraphClient | None,
    ) -> None:

        self._miner_metagraph_client = client

    async def fetch_completed_jobs(
        self,
        hotkeys: Iterable[str],
        *,
        include_empty: bool = True,
    ) -> Dict[str, list[Mapping[str, Any]]]:

        job_service = self._require_job_service()
        job_relay = getattr(job_service, "job_relay", None)
        if job_relay is None:
            raise RuntimeError("Configured job service does not expose a job relay client")

        hotkey_queue = self._normalize_hotkeys(hotkeys)
        if not hotkey_queue:
            derived = await self._derive_hotkeys_from_subnet()
            hotkey_queue = derived

        if not hotkey_queue:
            return {}

        semaphore = asyncio.Semaphore(self._fetch_concurrency)

        async def _fetch_one(hotkey: str) -> list[Mapping[str, Any]]:
            async with semaphore:
                try:
                    jobs = await job_relay.list_jobs_for_hotkey(hotkey)
                except Exception as exc:  # pragma: no cover - network/logging safeguard
                    logger.warning(
                        "score_service.fetch_failed hotkey=%s", hotkey, exc_info=exc
                    )
                    return []
                return jobs if isinstance(jobs, list) else []

        tasks = [_fetch_one(hotkey) for hotkey in hotkey_queue]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        completed: Dict[str, list[Mapping[str, Any]]] = {}
        for hotkey, result in zip(hotkey_queue, results):
            if isinstance(result, Exception):
                logger.warning(
                    "score_service.fetch_errored hotkey=%s error=%s",
                    hotkey,
                    result,
                )
                completed[hotkey] = [] if include_empty else []
                continue

            filtered = [job for job in result if self._is_completed_job(job)]
            if filtered or include_empty:
                completed[hotkey] = filtered

        return completed

    def jobs_to_score(
        self,
        jobs_by_hotkey: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> Dict[str, ScoreRecord]:

        scores: Dict[str, ScoreRecord] = {}
        for hotkey, payload in jobs_by_hotkey.items():
            completed_count = 0
            for job in payload:
                if self._is_completed_job(job):
                    completed_count += 1
            scores[hotkey] = ScoreRecord(scores=float(completed_count), is_slashed=False)
        return scores

    def update_scores(
        self,
        scores_by_hotkey: Mapping[str, ScoreRecord | float | int],
    ) -> bool:

        if not scores_by_hotkey:
            return True

        try:
            prepared: Dict[str, ScoreRecord] = {}
            for hotkey, value in scores_by_hotkey.items():
                if isinstance(value, ScoreRecord):
                    prepared[hotkey] = value
                else:
                    prepared[hotkey] = ScoreRecord(scores=float(value), is_slashed=False)
            self.put_many(prepared)
            return True
        except Exception as exc:  # pragma: no cover - persistence failure logging
            logger.error("score_service.update_scores_failed", exc_info=exc)
            return False

    def update_scoeres(
        self,
        scores_by_hotkey: Mapping[str, ScoreRecord | float | int],
    ) -> bool:
        """Compat wrapper for spec typo."""

        return self.update_scores(scores_by_hotkey)

    async def trigger(self) -> bool:

        scoped_hotkeys = await self._resolve_hotkeys()
        if not scoped_hotkeys:
            return False

        completed_jobs = await self.fetch_completed_jobs(scoped_hotkeys)
        if not completed_jobs:
            completed_jobs = {hotkey: [] for hotkey in scoped_hotkeys}

        score_records = self.jobs_to_score(completed_jobs)
        for hotkey in scoped_hotkeys:
            score_records.setdefault(hotkey, ScoreRecord(scores=0.0, is_slashed=False))

        return self.update_scores(score_records)

    def _require_job_service(self) -> "JobService":
        if self._job_service is None:
            raise RuntimeError("ScoreService job service dependency has not been configured")
        return self._job_service

    def _normalize_hotkeys(self, hotkeys: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        normalized: list[str] = []
        for hotkey in hotkeys:
            item = str(hotkey).strip()
            if not item or item in seen:
                continue
            seen.add(item)
            normalized.append(item)
        return normalized

    async def _resolve_hotkeys(self) -> list[str]:
        if self._miner_metagraph_client is not None:
            state = self._miner_metagraph_client.fetch_miners()
            if state:
                return list(state.keys())

        return await self._derive_hotkeys_from_subnet()

    async def _derive_hotkeys_from_subnet(self) -> list[str]:
        if (
            self._subnet_state_service is None
            or self._netuid is None
            or self._network is None
        ):
            return []

        try:
            state_result = await asyncio.to_thread(
                self._subnet_state_service.fetch_state,
                self._netuid,
                self._network,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("score_service.subnet_fetch_failed", exc_info=exc)
            return []

        if not state_result:
            return []

        state, _block = state_result
        if not state:
            return []

        return list(state.keys())

    @classmethod
    def _is_completed_job(cls, job: Mapping[str, Any]) -> bool:
        status = str(job.get("status", "")).lower()
        return status in cls._COMPLETED_STATUSES


_DEFAULT_LOOKBACK_WINDOW = timedelta(days=7)
_COMPLETED_INFERENCE_STATUSES = {"success"}
_HOTKEY_FETCH_CONCURRENCY = 8


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    return None


def _is_completed_inference_job(job: Mapping[str, Any], cutoff: datetime) -> bool:
    job_type = str(job.get("job_type") or "").lower()
    if job_type != "inference":
        return False

    if job.get("is_audit_job") is True:
        return False

    status = str(job.get("status") or "").lower()
    if status not in _COMPLETED_INFERENCE_STATUSES:
        return False

    completed_at = _parse_datetime(job.get("completed_at"))
    if completed_at is None or completed_at < cutoff:
        return False

    return True


async def build_scores_from_state(
    state: Dict[str, Miner],
    *,
    job_relay_client: BaseJobRelayClient,
    source: str = "metagraph",
    lookback_window: timedelta | None = None,
    score_fn: Callable[[Mapping[str, Any]], float] = job_to_score,
    fetch_concurrency: int = _HOTKEY_FETCH_CONCURRENCY,
) -> ScoresResponse:
    if job_relay_client is None:
        raise ValueError("job_relay_client must be provided for score construction")

    window = lookback_window or _DEFAULT_LOOKBACK_WINDOW
    cutoff = datetime.now(timezone.utc) - window
    concurrency = max(1, int(fetch_concurrency)) if fetch_concurrency else 1
    semaphore = asyncio.Semaphore(concurrency)

    async def _score_hotkey(hotkey: str, miner: Miner) -> tuple[str, ScorePayload, dict[str, Any]]:
        async with semaphore:
            try:
                jobs = await job_relay_client.list_jobs_for_hotkey(hotkey)
            except Exception as exc:  # pragma: no cover - defensive network logging
                logger.warning(
                    "scores.build_from_state.fetch_failed hotkey=%s",
                    hotkey,
                    exc_info=exc,
                )
                payload = ScorePayload(
                    status="COMPLETED",
                    score=ScoreValue(total_score=0.0),
                )
                return hotkey, payload, {
                    "jobs_considered": 0,
                    "jobs_scored": 0,
                    "fetch_failed": True,
                    "miner_valid": bool(getattr(miner, "valid", False)),
                }

            completed_jobs = [
                job for job in jobs if _is_completed_inference_job(job, cutoff)
            ]

            total_score = 0.0
            jobs_scored = 0
            for job in completed_jobs:
                try:
                    score_value = float(score_fn(job))
                except Exception as exc:  # pragma: no cover - scoring failure logging
                    logger.warning(
                        "scores.build_from_state.score_failed hotkey=%s job_id=%s",
                        hotkey,
                        job.get("job_id", "unknown"),
                        exc_info=exc,
                    )
                    continue

                if math.isnan(score_value) or math.isinf(score_value):
                    logger.warning(
                        "scores.build_from_state.score_invalid hotkey=%s job_id=%s score=%s",
                        hotkey,
                        job.get("job_id", "unknown"),
                        score_value,
                    )
                    continue

                total_score += max(score_value, 0.0)
                jobs_scored += 1

            payload = ScorePayload(
                status="COMPLETED",
                score=ScoreValue(total_score=total_score),
            )
            return hotkey, payload, {
                "jobs_considered": len(completed_jobs),
                "jobs_scored": jobs_scored,
                "fetch_failed": False,
                "miner_valid": bool(getattr(miner, "valid", False)),
            }

    if not state:
        stats = {
            "source": source,
            "requested": 0,
            "available": 0,
            "jobs_considered": 0,
            "jobs_scored": 0,
            "fetch_failures": 0,
            "lookback_window_seconds": int(window.total_seconds()),
        }
        return ScoresResponse(scores={}, stats=stats)

    tasks = [
        _score_hotkey(hotkey, miner)
        for hotkey, miner in state.items()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    scores: Dict[str, ScorePayload] = {}
    jobs_considered_total = 0
    jobs_scored_total = 0
    fetch_failures = 0
    valid_miners = 0

    for hotkey, payload, metadata in results:
        scores[hotkey] = payload
        jobs_considered_total += int(metadata["jobs_considered"])
        jobs_scored_total += int(metadata["jobs_scored"])
        if metadata["fetch_failed"]:
            fetch_failures += 1
        if metadata["miner_valid"]:
            valid_miners += 1

    stats = {
        "source": source,
        "requested": len(state),
        "available": len(scores),
        "jobs_considered": jobs_considered_total,
        "jobs_scored": jobs_scored_total,
        "fetch_failures": fetch_failures,
        "valid_miners": valid_miners,
        "lookback_window_seconds": int(window.total_seconds()),
    }

    return ScoresResponse(scores=scores, stats=stats)


__all__ = [
    "build_scores_from_state",
    "ScoreRecord",
    "ScoreService",
]
