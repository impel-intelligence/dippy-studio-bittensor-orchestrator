from __future__ import annotations

import asyncio
import json
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from psycopg.types.json import Json

try:
    from pydantic import BaseModel, ConfigDict
except ImportError:
    from pydantic import BaseModel  # type: ignore

    ConfigDict = None  # type: ignore

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.common.model_utils import dump_model, validate_model
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


@dataclass
class ScoreRunSummary:
    timestamp: datetime
    hotkeys_considered: int
    hotkeys_updated: int
    zeroed_hotkeys: int
    jobs_considered: int
    success: bool
    trace_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class ScoreRepository:
    """Persist and retrieve miner scores."""

    _TIMESTAMP_FIELDS = (
        "ema_last_update_at",
        "last_sample_at",
        "last_score_update_at",
        "last_updated_at",
    )

    def __init__(self, database_service: DatabaseService) -> None:
        self._database_service = database_service
        self._ensure_schema()
        self._last_update: Optional[datetime] = None

    @property
    def db_path(self) -> str:
        return self._database_service.safe_dsn

    def last_update(self) -> Optional[datetime]:
        if self._last_update is not None:
            return self._last_update

        with self._database_service.cursor() as cur:
            cur.execute("SELECT scores FROM miners WHERE scores IS NOT NULL")
            rows = cur.fetchall()

        for payload, in rows:
            try:
                self._payload_to_record(payload)
            except Exception as exc:  # noqa: BLE001
                logger.debug("score_repository.last_update.parse_failed", exc_info=exc)

        return self._last_update

    def put(self, hotkey: str, record: Mapping[str, Any] | ScoreRecord) -> None:
        self.put_many({hotkey: record})

    def put_many(self, records: Mapping[str, Mapping[str, Any] | ScoreRecord]) -> None:
        if not records:
            return

        prepared = [self._prepare_row(hotkey, value) for hotkey, value in records.items()]
        if not prepared:
            return

        with self._database_service.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO miners (hotkey, value, scores)
                VALUES (%s, %s, %s)
                ON CONFLICT(hotkey) DO UPDATE SET scores = EXCLUDED.scores
                """,
                prepared,
            )
        self._last_update = datetime.now(timezone.utc)

    def replace_all(self, records: Mapping[str, Mapping[str, Any] | ScoreRecord]) -> None:
        with self._database_service.cursor() as cur:
            cur.execute("UPDATE miners SET scores = NULL")
        if records:
            self.put_many(records)
        else:
            self._last_update = datetime.now(timezone.utc)

    def delete(self, hotkey: str) -> None:
        with self._database_service.cursor() as cur:
            cur.execute(
                "UPDATE miners SET scores = NULL WHERE hotkey = %s",
                (hotkey,),
            )
        self._last_update = datetime.now(timezone.utc)

    def get(self, hotkey: str) -> Optional[ScoreRecord]:
        with self._database_service.cursor() as cur:
            cur.execute(
                "SELECT scores FROM miners WHERE hotkey = %s",
                (hotkey,),
            )
            row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return self._payload_to_record(row[0])

    def all(self) -> Dict[str, ScoreRecord]:
        with self._database_service.cursor() as cur:
            cur.execute("SELECT hotkey, scores FROM miners WHERE scores IS NOT NULL")
            rows = cur.fetchall()
        return {hotkey: self._payload_to_record(payload) for hotkey, payload in rows}

    def items(self) -> Iterable[Tuple[str, ScoreRecord]]:
        with self._database_service.cursor() as cur:
            cur.execute("SELECT hotkey, scores FROM miners WHERE scores IS NOT NULL")
            rows = cur.fetchall()
        for hotkey, payload in rows:
            yield hotkey, self._payload_to_record(payload)

    def clear(self) -> None:
        with self._database_service.cursor() as cur:
            cur.execute("UPDATE miners SET scores = NULL")
        self._last_update = datetime.now(timezone.utc)

    def _ensure_schema(self) -> None:
        with self._database_service.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE miners
                ADD COLUMN IF NOT EXISTS scores JSONB
                """
            )

    def _prepare_row(self, hotkey: str, value: Mapping[str, Any] | ScoreRecord) -> Tuple[str, Json, Json]:
        record = self._coerce_record(value)
        return hotkey, Json({}), Json(self._record_to_payload(record))

    def _coerce_record(self, value: Mapping[str, Any] | ScoreRecord) -> ScoreRecord:
        if isinstance(value, ScoreRecord):
            return value

        data = dump_model(value)

        if "scores" not in data:
            raise ValueError("score payload must include a 'scores' field")
        if "is_slashed" not in data:
            raise ValueError("score payload must include an 'is_slashed' field")

        data = {
            **data,
            "scores": float(data["scores"]),
            "is_slashed": bool(data["is_slashed"]),
        }

        return validate_model(ScoreRecord, data)

    def _record_to_payload(self, record: ScoreRecord) -> Dict[str, Any]:
        return dump_model(record)

    def _payload_to_record(self, payload: object) -> ScoreRecord:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            payload = payload.decode()
        if isinstance(payload, str):
            data = json.loads(payload)
        else:
            data = payload
        record = validate_model(ScoreRecord, data)
        self._register_record_update(record)
        return record

    def _register_record_update(self, record: ScoreRecord) -> None:
        candidate = self._extract_record_timestamp(record)
        if candidate is None:
            return
        if self._last_update is None or candidate > self._last_update:
            self._last_update = candidate

    def _extract_record_timestamp(self, record: ScoreRecord) -> Optional[datetime]:
        try:
            data = dump_model(record)
        except Exception:
            data = {}

        candidates: list[datetime] = []
        for field in self._TIMESTAMP_FIELDS:
            value = data.get(field)
            parsed = self._parse_datetime(value)
            if parsed is not None:
                candidates.append(parsed)

        if candidates:
            return max(candidates)
        return None

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        return None


class ScoreEngine:
    """Coordinate score collection and persistence."""

    _COMPLETED_STATUSES = {"success", "failed", "timeout"}

    def __init__(
        self,
        repository: ScoreRepository,
        *,
        job_service: "JobService | None" = None,
        subnet_state_service: "SubnetStateService | None" = None,
        miner_metagraph_client: LiveMinerMetagraphClient | None = None,
        netuid: int | None = None,
        network: str | None = None,
        fetch_concurrency: int = 8,
        score_settings: "ScoreSettings | None" = None,
        score_fn: Callable[[Mapping[str, Any]], float] = job_to_score,
        lookback_days: float | int | None = 7,
    ) -> None:
        self._repository = repository
        self._job_service = job_service
        self._subnet_state_service = subnet_state_service
        self._miner_metagraph_client = miner_metagraph_client
        self._netuid = netuid
        self._network = network
        self._fetch_concurrency = max(1, int(fetch_concurrency)) if fetch_concurrency else 1
        self._score_settings = (score_settings or ScoreSettings()).normalized()
        self._score_fn = score_fn
        self._lookback_days: float | None = None
        self._lookback_window: timedelta | None = None
        if lookback_days is not None:
            try:
                normalized_window = float(lookback_days)
            except (TypeError, ValueError):
                normalized_window = 0.0
            if normalized_window > 0.0:
                self._lookback_days = normalized_window
                self._lookback_window = timedelta(days=normalized_window)

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

    def set_miner_metagraph_client(self, client: LiveMinerMetagraphClient | None) -> None:
        self._miner_metagraph_client = client

    async def fetch_completed_jobs(
        self,
        hotkeys: Iterable[str],
        *,
        include_empty: bool = True,
        lookback_days: float | None = None,
        reference_time: datetime | None = None,
    ) -> tuple[Dict[str, list[Mapping[str, Any]]], set[str]]:
        job_service = self._require_job_service()
        job_relay = getattr(job_service, "job_relay", None)
        if job_relay is None:
            raise RuntimeError("Configured job service does not expose a job relay client")

        hotkey_queue = self._normalize_hotkeys(hotkeys)
        if not hotkey_queue:
            derived = await self._derive_hotkeys_from_subnet()
            hotkey_queue = derived

        if not hotkey_queue:
            return {}, set()

        reference = reference_time or datetime.now(timezone.utc)
        window_delta: timedelta | None
        if lookback_days is None:
            window_delta = self._lookback_window
        else:
            try:
                window_value = float(lookback_days)
            except (TypeError, ValueError):
                window_value = 0.0
            window_delta = timedelta(days=window_value) if window_value > 0.0 else None
        cutoff_time: datetime | None = None
        if window_delta is not None:
            cutoff_time = reference - window_delta

        semaphore = asyncio.Semaphore(self._fetch_concurrency)

        async def _fetch_one(hotkey: str) -> tuple[list[Mapping[str, Any]], bool]:
            async with semaphore:
                try:
                    jobs = await job_relay.list_jobs_for_hotkey(hotkey, since=cutoff_time)
                except Exception as exc:  # pragma: no cover - network/logging safeguard
                    logger.warning("score_engine.fetch_failed hotkey=%s", hotkey, exc_info=exc)
                    return [], True
                if not isinstance(jobs, list):
                    logger.warning(
                        "score_engine.fetch_invalid_payload hotkey=%s type=%s",
                        hotkey,
                        type(jobs).__name__,
                    )
                    return [], True
                return jobs, False

        tasks = [_fetch_one(hotkey) for hotkey in hotkey_queue]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        completed: Dict[str, list[Mapping[str, Any]]] = {}
        failures: set[str] = set()
        for hotkey, result in zip(hotkey_queue, results):
            if isinstance(result, Exception):
                logger.warning(
                    "score_engine.fetch_errored hotkey=%s error=%s",
                    hotkey,
                    result,
                )
                if include_empty:
                    completed[hotkey] = []
                failures.add(hotkey)
                continue

            jobs, failed = result
            if failed:
                failures.add(hotkey)
            filtered = [job for job in jobs if self._is_completed_job(job)]
            if filtered or include_empty:
                completed[hotkey] = filtered

        return completed, failures

    def jobs_to_score(
        self,
        jobs_by_hotkey: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        reference_time: datetime | None = None,
        existing_records: Mapping[str, Mapping[str, Any] | ScoreRecord] | None = None,
        hotkeys: Iterable[str] | None = None,
    ) -> Dict[str, ScoreRecord]:
        if not jobs_by_hotkey and not existing_records and not hotkeys:
            return {}

        now = reference_time or datetime.now(timezone.utc)
        stored = existing_records or {}

        key_set: set[str] = set(self._normalize_hotkeys(jobs_by_hotkey.keys()))
        if hotkeys is not None:
            key_set.update(self._normalize_hotkeys(hotkeys))
        if stored:
            key_set.update(self._normalize_hotkeys(stored.keys()))

        results: Dict[str, ScoreRecord] = {}
        for hotkey in sorted(key_set):
            history = self._build_history(
                hotkey=hotkey,
                jobs=jobs_by_hotkey.get(hotkey, ()),
                existing_record=stored.get(hotkey),
                reference_time=now,
            )
            results[hotkey] = history.to_record()

        return results

    def update_scores(self, scores_by_hotkey: Mapping[str, ScoreRecord | float | int]) -> bool:
        if not scores_by_hotkey:
            return True

        try:
            prepared: Dict[str, ScoreRecord] = {}
            for hotkey, value in scores_by_hotkey.items():
                if isinstance(value, ScoreRecord):
                    prepared[hotkey] = value
                else:
                    prepared[hotkey] = ScoreRecord(scores=float(value), is_slashed=False)
            self._repository.put_many(prepared)
            return True
        except Exception as exc:  # pragma: no cover - persistence failure logging
            logger.error("score_engine.update_scores_failed", exc_info=exc)
            return False

    async def run_once(self, *, trace_hotkeys: Iterable[str] | None = None) -> "ScoreRunSummary | None":
        trace_targets = self._normalize_hotkeys(trace_hotkeys or [])
        trace_details: Dict[str, Dict[str, Any]] = {}
        if trace_targets:
            logger.info(
                "score_engine.trace.requested hotkeys=%s",
                ",".join(trace_targets),
            )

        scoped_hotkeys = await self._resolve_hotkeys()
        if scoped_hotkeys:
            normalized_hotkeys = self._normalize_hotkeys(scoped_hotkeys)
        else:
            normalized_hotkeys = []

        if trace_targets:
            normalized_hotkeys = self._normalize_hotkeys(normalized_hotkeys + trace_targets)

        if not normalized_hotkeys:
            logger.info("score_engine.run.skipped reason=%s", "no_hotkeys")
            return None

        preview = normalized_hotkeys[:5]
        logger.info(
            "score_engine.hotkeys.resolved count=%s preview=%s extra=%s",
            len(normalized_hotkeys),
            preview,
            max(0, len(normalized_hotkeys) - len(preview)),
        )

        window_reference = datetime.now(timezone.utc)
        completed_jobs, fetch_failures = await self.fetch_completed_jobs(
            normalized_hotkeys,
            lookback_days=self._lookback_days,
            reference_time=window_reference,
        )
        for hotkey in normalized_hotkeys:
            completed_jobs.setdefault(hotkey, [])

        jobs_considered = sum(len(jobs) for jobs in completed_jobs.values())

        logger.info(
            "score_engine.jobs.collected hotkeys=%s completed_jobs=%s",
            len(completed_jobs),
            jobs_considered,
        )

        if trace_targets:
            for hotkey in trace_targets:
                jobs = completed_jobs.get(hotkey, [])
                statuses = sorted({str(job.get("status", "")).lower() or "unknown" for job in jobs})
                trace_details[hotkey] = {
                    "jobs_fetched": len(jobs),
                    "job_statuses": statuses,
                    "considered": hotkey in normalized_hotkeys,
                    "fetch_failed": hotkey in fetch_failures,
                }
                logger.info(
                    "score_engine.trace.jobs hotkey=%s count=%s statuses=%s",
                    hotkey,
                    len(jobs),
                    statuses,
                )

        stored_scores = self._repository.all()
        existing = {hotkey: stored_scores[hotkey] for hotkey in normalized_hotkeys if hotkey in stored_scores}

        reference_now = datetime.now(timezone.utc)

        score_records = self.jobs_to_score(
            completed_jobs,
            reference_time=reference_now,
            existing_records=existing,
            hotkeys=normalized_hotkeys,
        )
        for hotkey in normalized_hotkeys:
            score_records.setdefault(hotkey, ScoreRecord(scores=0.0, is_slashed=False))
            if hotkey in fetch_failures:
                continue
            if completed_jobs.get(hotkey):
                continue

            current_record = score_records.get(hotkey)
            is_slashed = False
            if isinstance(current_record, ScoreRecord):
                is_slashed = bool(current_record.is_slashed)
            elif isinstance(current_record, Mapping):
                is_slashed = bool(current_record.get("is_slashed"))

            reset_history = ScoreHistory(
                settings=self._fresh_settings(),
                is_slashed=is_slashed,
                extra_fields={"hotkey": hotkey},
            )
            reset_history.apply_decay(reference_now)
            score_records[hotkey] = reset_history.to_record()

        if trace_targets:
            for hotkey in trace_targets:
                record = score_records.get(hotkey)
                record_score = float(record.scores) if record else None
                payload = trace_details.setdefault(
                    hotkey,
                    {
                        "considered": hotkey in normalized_hotkeys,
                        "jobs_fetched": len(completed_jobs.get(hotkey, [])),
                        "job_statuses": sorted(
                            {str(job.get("status", "")).lower() or "unknown" for job in completed_jobs.get(hotkey, [])}
                        ),
                        "fetch_failed": hotkey in fetch_failures,
                    },
                )
                if record is not None:
                    payload["score"] = record_score
                    payload["slashed"] = bool(record.is_slashed)
                else:
                    payload.setdefault("score", None)
                    payload.setdefault("slashed", None)
                logger.info(
                    "score_engine.trace.scores hotkey=%s score=%s slashed=%s",
                    hotkey,
                    record_score,
                    getattr(record, "is_slashed", None) if record else None,
                )

        zeroed_hotkeys = sum(1 for record in score_records.values() if float(record.scores) <= 0.0)

        persisted = self.update_scores(score_records)

        summary = ScoreRunSummary(
            timestamp=reference_now,
            hotkeys_considered=len(normalized_hotkeys),
            hotkeys_updated=len(score_records),
            zeroed_hotkeys=zeroed_hotkeys,
            jobs_considered=jobs_considered,
            success=persisted,
            trace_details=trace_details,
        )

        if persisted:
            logger.info(
                "score_engine.run.complete hotkeys=%s updated=%s zeroed=%s jobs=%s",
                summary.hotkeys_considered,
                summary.hotkeys_updated,
                summary.zeroed_hotkeys,
                summary.jobs_considered,
            )
        else:
            logger.error(
                "score_engine.run.persist_failed hotkeys=%s jobs=%s",
                summary.hotkeys_considered,
                summary.jobs_considered,
            )

        return summary

    def _build_history(
        self,
        *,
        hotkey: str,
        jobs: Sequence[Mapping[str, Any]],
        existing_record: Mapping[str, Any] | ScoreRecord | None,
        reference_time: datetime,
    ) -> "ScoreHistory":
        history = ScoreHistory.from_jobs(
            jobs,
            existing_record=existing_record,
            score_fn=self._score_fn,
            settings=self._score_settings,
            reference_time=reference_time,
            logger=logger,
        )
        if hotkey:
            history.extra_fields.setdefault("hotkey", hotkey)
        return history

    def _fresh_settings(self) -> "ScoreSettings":
        return ScoreSettings(
            ema_alpha=self._score_settings.ema_alpha,
            ema_half_life_seconds=self._score_settings.ema_half_life_seconds,
            failure_penalty_weight=self._score_settings.failure_penalty_weight,
        ).normalized()

    def _require_job_service(self) -> "JobService":
        if self._job_service is None:
            raise RuntimeError("ScoreEngine job service dependency has not been configured")
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
            logger.warning("score_engine.subnet_fetch_failed", exc_info=exc)
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
class ScoreService:
    """Facade that exposes legacy ScoreService API while delegating to repository/engine."""

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
        ema_alpha: float = 1.0,
        ema_half_life_seconds: float = 604_800.0,
        failure_penalty_weight: float = 0.2,
        score_fn: Callable[[Mapping[str, Any]], float] = job_to_score,
        lookback_days: float | int | None = 7,
    ) -> None:
        settings = ScoreSettings(
            ema_alpha=ema_alpha,
            ema_half_life_seconds=ema_half_life_seconds,
            failure_penalty_weight=failure_penalty_weight,
        ).normalized()

        self._repository = ScoreRepository(database_service)
        self._lookback_days: float | None
        if lookback_days is None:
            self._lookback_days = None
        else:
            try:
                candidate = float(lookback_days)
            except (TypeError, ValueError):
                candidate = 0.0
            self._lookback_days = candidate if candidate > 0.0 else None
        self._engine = ScoreEngine(
            repository=self._repository,
            job_service=job_service,
            subnet_state_service=subnet_state_service,
            miner_metagraph_client=miner_metagraph_client,
            netuid=netuid,
            network=network,
            fetch_concurrency=fetch_concurrency,
            score_settings=settings,
            score_fn=score_fn,
            lookback_days=self._lookback_days,
        )

    @property
    def db_path(self) -> str:
        return self._repository.db_path

    def last_update(self) -> Optional[datetime]:
        return self._repository.last_update()

    def put(self, hotkey: str, record: Mapping[str, Any] | ScoreRecord) -> None:
        self._repository.put(hotkey, record)

    def put_many(self, records: Mapping[str, Mapping[str, Any] | ScoreRecord]) -> None:
        self._repository.put_many(records)

    def replace_all(self, records: Mapping[str, Mapping[str, Any] | ScoreRecord]) -> None:
        self._repository.replace_all(records)

    def delete(self, hotkey: str) -> None:
        self._repository.delete(hotkey)

    def get(self, hotkey: str) -> Optional[ScoreRecord]:
        return self._repository.get(hotkey)

    def all(self) -> Dict[str, ScoreRecord]:
        return self._repository.all()

    def items(self) -> Iterable[Tuple[str, ScoreRecord]]:
        return self._repository.items()

    def clear(self) -> None:
        self._repository.clear()

    def set_job_service(self, job_service: "JobService | None") -> None:
        self._engine.set_job_service(job_service)

    def set_subnet_state_service(
        self,
        subnet_state_service: "SubnetStateService | None",
        *,
        netuid: int | None = None,
        network: str | None = None,
    ) -> None:
        self._engine.set_subnet_state_service(
            subnet_state_service,
            netuid=netuid,
            network=network,
        )

    def set_miner_metagraph_client(self, client: LiveMinerMetagraphClient | None) -> None:
        self._engine.set_miner_metagraph_client(client)

    async def fetch_completed_jobs(
        self,
        hotkeys: Iterable[str],
        *,
        include_empty: bool = True,
        lookback_days: float | None = None,
    ) -> tuple[Dict[str, list[Mapping[str, Any]]], set[str]]:
        window = lookback_days if lookback_days is not None else self._lookback_days
        return await self._engine.fetch_completed_jobs(
            hotkeys,
            include_empty=include_empty,
            lookback_days=window,
        )

    def jobs_to_score(
        self,
        jobs_by_hotkey: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        reference_time: datetime | None = None,
        existing_records: Mapping[str, Mapping[str, Any] | ScoreRecord] | None = None,
        hotkeys: Iterable[str] | None = None,
    ) -> Dict[str, ScoreRecord]:
        return self._engine.jobs_to_score(
            jobs_by_hotkey,
            reference_time=reference_time,
            existing_records=existing_records,
            hotkeys=hotkeys,
        )

    def update_scores(
        self,
        scores_by_hotkey: Mapping[str, ScoreRecord | float | int],
    ) -> bool:
        return self._engine.update_scores(scores_by_hotkey)

    def update_scoeres(
        self,
        scores_by_hotkey: Mapping[str, ScoreRecord | float | int],
    ) -> bool:
        return self.update_scores(scores_by_hotkey)

    async def run_once(self, *, trace_hotkeys: Iterable[str] | None = None) -> "ScoreRunSummary | None":
        return await self._engine.run_once(trace_hotkeys=trace_hotkeys)

    @classmethod
    def _is_completed_job(cls, job: Mapping[str, Any]) -> bool:
        return ScoreEngine._is_completed_job(job)



@dataclass
class ScoreSettings:
    ema_alpha: float = 1.0
    ema_half_life_seconds: float = 604_800.0
    failure_penalty_weight: float = 0.2

    def normalized(self) -> "ScoreSettings":
        alpha = max(0.0, min(float(self.ema_alpha), 1.0))
        half_life = float(self.ema_half_life_seconds)
        if half_life <= 0.0:
            half_life = 604_800.0
        penalty = float(self.failure_penalty_weight)
        if penalty < 0.0:
            penalty = 0.0
        return ScoreSettings(
            ema_alpha=alpha,
            ema_half_life_seconds=half_life,
            failure_penalty_weight=penalty,
        )


@dataclass
class ScoreHistory:
    settings: ScoreSettings
    scores: float = 0.0
    ema_score: float = 0.0
    legacy_score: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    sample_count: int = 0
    last_success_at: datetime | None = None
    last_ema_update_at: datetime | None = None
    is_slashed: bool = False
    extra_fields: dict[str, Any] = field(default_factory=dict)

    def apply_decay(self, reference_time: datetime) -> None:
        reference = self._ensure_aware(reference_time)
        baseline = self.last_ema_update_at or self.last_success_at
        if baseline is None:
            self.last_ema_update_at = reference
            return

        elapsed = (reference - baseline).total_seconds()
        if elapsed <= 0:
            self.last_ema_update_at = reference
            return

        half_life = max(self.settings.ema_half_life_seconds, 1.0)
        decay_factor = 0.5 ** (elapsed / half_life)
        self.ema_score = self._clamp_score(self.ema_score * decay_factor)
        if self.scores > self.ema_score:
            self.scores = self.ema_score
        self.last_ema_update_at = reference

    def register_success(self, sample: float, event_time: datetime, job: Mapping[str, Any] | None = None) -> None:
        value = self._clamp_score(sample)
        event = self._ensure_aware(event_time)
        alpha = max(0.0, min(self.settings.ema_alpha, 1.0))
        if alpha >= 1.0:
            ema = value
        elif alpha <= 0.0:
            ema = self.ema_score
        else:
            ema = alpha * value + (1.0 - alpha) * self.ema_score
        self.ema_score = self._clamp_score(ema)
        self.scores = self.ema_score
        self.sample_count += 1
        self.success_count += 1
        self.last_success_at = event
        self.last_ema_update_at = event
        self.legacy_score = self._compute_legacy()
        if job is not None:
            job_id = job.get('job_id')
            if job_id is not None:
                self.extra_fields['last_job_id'] = job_id
        self.extra_fields['failure_penalty_weight'] = self.settings.failure_penalty_weight
        self.extra_fields['decay_half_life_seconds'] = self.settings.ema_half_life_seconds
        self.extra_fields['ema_alpha'] = self.settings.ema_alpha

    def register_failure(self, event_time: datetime, job: Mapping[str, Any] | None = None) -> None:
        event = self._ensure_aware(event_time)
        self.failure_count += 1
        self.last_ema_update_at = event
        self.legacy_score = self._compute_legacy()
        candidate = min(self.ema_score, self.legacy_score)
        if self.scores > 0.0:
            candidate = min(self.scores, candidate)
        self.scores = max(candidate, 0.0)
        if job is not None:
            job_id = job.get('job_id')
            if job_id is not None:
                self.extra_fields['last_job_id'] = job_id

    def to_payload(self) -> dict[str, Any]:
        payload = {**self.extra_fields}
        payload.update(
            {
                'scores': float(self.scores),
                'is_slashed': bool(self.is_slashed),
                'ema_score': float(self.ema_score),
                'legacy_score': float(self.legacy_score),
                'success_count': int(self.success_count),
                'failure_count': int(self.failure_count),
                'sample_count': int(self.sample_count),
                'last_sample_at': self.last_success_at.isoformat() if self.last_success_at else None,
                'ema_last_update_at': self.last_ema_update_at.isoformat() if self.last_ema_update_at else None,
                'decay_half_life_seconds': float(self.settings.ema_half_life_seconds),
                'failure_penalty_weight': float(self.settings.failure_penalty_weight),
                'ema_alpha': float(self.settings.ema_alpha),
            }
        )
        return {key: value for key, value in payload.items() if value is not None}

    def to_record(self) -> 'ScoreRecord':
        return ScoreRecord(**self.to_payload())

    def _compute_legacy(self) -> float:
        base = float(self.success_count)
        penalty = float(self.settings.failure_penalty_weight) * float(self.failure_count)
        return self._clamp_non_negative(base - penalty)

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, Any] | ScoreRecord | None,
        *,
        settings: ScoreSettings,
    ) -> 'ScoreHistory':
        if record is None:
            payload: dict[str, Any] = {}
        else:
            payload = dump_model(record)

        scores = cls._coerce_float(payload.get('scores'), default=0.0)
        ema_score = cls._coerce_float(payload.get('ema_score'), default=scores)
        legacy_score = cls._coerce_float(payload.get('legacy_score'), default=scores)
        success_count = cls._coerce_int(payload.get('success_count'), default=0)
        failure_count = cls._coerce_int(payload.get('failure_count'), default=0)
        sample_count = cls._coerce_int(payload.get('sample_count'), default=success_count)
        last_success_at = cls._parse_datetime(payload.get('last_sample_at'))
        last_ema_update_at = cls._parse_datetime(
            payload.get('ema_last_update_at') or payload.get('last_score_update_at')
        )
        is_slashed = bool(payload.get('is_slashed', False))

        managed_keys = {
            'scores',
            'is_slashed',
            'ema_score',
            'legacy_score',
            'success_count',
            'failure_count',
            'sample_count',
            'last_sample_at',
            'ema_last_update_at',
            'decay_half_life_seconds',
            'failure_penalty_weight',
            'ema_alpha',
            'last_score_update_at',
        }
        extras = {key: value for key, value in payload.items() if key not in managed_keys}

        normalized_settings = settings.normalized()

        history = cls(
            settings=normalized_settings,
            scores=cls._clamp_non_negative(scores),
            ema_score=cls._clamp_score(ema_score),
            legacy_score=cls._clamp_non_negative(legacy_score),
            success_count=success_count,
            failure_count=failure_count,
            sample_count=sample_count,
            last_success_at=last_success_at,
            last_ema_update_at=last_ema_update_at,
            is_slashed=is_slashed,
            extra_fields=extras,
        )

        stored_half_life = cls._coerce_float(payload.get('decay_half_life_seconds'), default=None)
        if stored_half_life is not None and stored_half_life > 0.0:
            history.settings.ema_half_life_seconds = stored_half_life
        stored_penalty = cls._coerce_float(payload.get('failure_penalty_weight'), default=None)
        if stored_penalty is not None and stored_penalty >= 0.0:
            history.settings.failure_penalty_weight = stored_penalty
        stored_alpha = cls._coerce_float(payload.get('ema_alpha'), default=None)
        if stored_alpha is not None:
            history.settings.ema_alpha = max(0.0, min(stored_alpha, 1.0))
        history.settings = history.settings.normalized()
        history.legacy_score = history._compute_legacy()
        if history.scores == 0.0 and history.ema_score > 0.0:
            history.scores = history.ema_score
        return history

    @classmethod
    def from_jobs(
        cls,
        jobs: Sequence[Mapping[str, Any]],
        *,
        existing_record: Mapping[str, Any] | ScoreRecord | None,
        score_fn: Callable[[Mapping[str, Any]], float],
        settings: ScoreSettings,
        reference_time: datetime,
        logger: logging.Logger | None = None,
    ) -> 'ScoreHistory':
        history = cls.from_record(existing_record, settings=settings)
        history.scores = 0.0
        history.ema_score = 0.0
        history.legacy_score = 0.0
        history.success_count = 0
        history.failure_count = 0
        history.sample_count = 0
        history.last_success_at = None
        history.last_ema_update_at = None
        history.extra_fields.pop('last_job_id', None)
        events: list[tuple[datetime, Mapping[str, Any]]] = []
        for job in jobs:
            if not ScoreService._is_completed_job(job):
                continue
            event_time = cls._extract_event_time(job)
            events.append((event_time or reference_time, job))

        events.sort(key=lambda item: item[0])
        for event_time, job in events:
            history.apply_decay(event_time)
            status = str(job.get('status', '')).lower()
            if status == 'success':
                try:
                    raw_score = score_fn(job)
                except Exception as exc:
                    if logger is not None:
                        logger.warning(
                            'score_history.sample_failed hotkey=%s job_id=%s',
                            job.get('miner_hotkey'),
                            job.get('job_id'),
                            exc_info=exc,
                        )
                    continue
                history.register_success(raw_score, event_time, job=job)
            else:
                history.register_failure(event_time, job=job)

        history.apply_decay(reference_time)
        return history

    @staticmethod
    def _clamp_score(value: float) -> float:
        if math.isnan(value) or math.isinf(value):
            return 0.0
        if value < 0.0:
            return 0.0
        if value > 1.0:
            return 1.0
        return value

    @staticmethod
    def _clamp_non_negative(value: float) -> float:
        if math.isnan(value) or math.isinf(value):
            return 0.0
        return value if value > 0.0 else 0.0

    @staticmethod
    def _coerce_float(value: Any, default: float | None = 0.0) -> float:
        if value in {None, '', 'None'}:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _coerce_int(value: Any, default: int = 0) -> int:
        if value in {None, '', 'None'}:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        return None

    @staticmethod
    def _extract_event_time(job: Mapping[str, Any]) -> datetime | None:
        candidates = (
            job.get('completed_at'),
            job.get('last_updated_at'),
            job.get('response_timestamp'),
            job.get('prepared_at'),
            job.get('creation_timestamp'),
        )
        for candidate in candidates:
            parsed = ScoreHistory._parse_datetime(candidate)
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _ensure_aware(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)



_DEFAULT_LOOKBACK_WINDOW = timedelta(days=7)
_COMPLETED_INFERENCE_STATUSES = {"success", "failed", "timeout"}
_SUCCESSFUL_INFERENCE_STATUSES = {"success"}
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


def _is_relevant_inference_job(job: Mapping[str, Any], cutoff: datetime) -> bool:
    job_type = str(job.get("job_type") or "").lower()
    if job_type != "inference":
        return False

    if job.get("is_audit_job") is True:
        return False

    status = str(job.get("status") or "").lower()
    if status not in _COMPLETED_INFERENCE_STATUSES:
        return False

    event_time = ScoreHistory._extract_event_time(job)
    if event_time is None or event_time < cutoff:
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
    ema_alpha: float = 1.0,
    ema_half_life_seconds: float | None = None,
    failure_penalty_weight: float = 0.2,
) -> ScoresResponse:
    if job_relay_client is None:
        raise ValueError("job_relay_client must be provided for score construction")

    window = lookback_window or _DEFAULT_LOOKBACK_WINDOW
    reference_now = datetime.now(timezone.utc)
    cutoff = reference_now - window
    concurrency = max(1, int(fetch_concurrency)) if fetch_concurrency else 1
    semaphore = asyncio.Semaphore(concurrency)

    settings = ScoreSettings(
        ema_alpha=ema_alpha,
        ema_half_life_seconds=(ema_half_life_seconds or window.total_seconds()),
        failure_penalty_weight=failure_penalty_weight,
    ).normalized()

    async def _score_hotkey(hotkey: str, miner: Miner) -> tuple[str, ScorePayload, dict[str, Any]]:
        async with semaphore:
            try:
                jobs = await job_relay_client.list_jobs_for_hotkey(hotkey, since=cutoff)
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
                    "failures": 0,
                    "fetch_failed": True,
                    "miner_valid": bool(getattr(miner, "valid", False)),
                }

            filtered_jobs = [
                job for job in jobs if _is_relevant_inference_job(job, cutoff)
            ]

            history = ScoreHistory.from_jobs(
                filtered_jobs,
                existing_record=None,
                score_fn=score_fn,
                settings=settings,
                reference_time=datetime.now(timezone.utc),
                logger=logger,
            )

            payload = ScorePayload(
                status="COMPLETED",
                score=ScoreValue(total_score=float(history.scores)),
            )
            return hotkey, payload, {
                "jobs_considered": len(filtered_jobs),
                "jobs_scored": history.success_count,
                "failures": history.failure_count,
                "fetch_failed": False,
                "miner_valid": bool(getattr(miner, "valid", False)),
                "ema_score": history.ema_score,
            }

    if not state:
        stats = {
            "source": source,
            "requested": 0,
            "available": 0,
            "jobs_considered": 0,
            "jobs_scored": 0,
            "failures_total": 0,
            "fetch_failures": 0,
            "lookback_window_seconds": int(window.total_seconds()),
            "ema_alpha": settings.ema_alpha,
            "ema_half_life_seconds": settings.ema_half_life_seconds,
            "failure_penalty_weight": settings.failure_penalty_weight,
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
    failures_total = 0
    fetch_failures = 0
    valid_miners = 0

    for hotkey, payload, metadata in results:
        scores[hotkey] = payload
        jobs_considered_total += int(metadata.get("jobs_considered", 0))
        jobs_scored_total += int(metadata.get("jobs_scored", 0))
        failures_total += int(metadata.get("failures", 0))
        if metadata.get("fetch_failed"):
            fetch_failures += 1
        if metadata.get("miner_valid"):
            valid_miners += 1

    stats = {
        "source": source,
        "requested": len(state),
        "available": len(scores),
        "jobs_considered": jobs_considered_total,
        "jobs_scored": jobs_scored_total,
        "failures_total": failures_total,
        "fetch_failures": fetch_failures,
        "valid_miners": valid_miners,
        "lookback_window_seconds": int(window.total_seconds()),
        "ema_alpha": settings.ema_alpha,
        "ema_half_life_seconds": settings.ema_half_life_seconds,
        "failure_penalty_weight": settings.failure_penalty_weight,
    }

    return ScoresResponse(scores=scores, stats=stats)


__all__ = [
    "build_scores_from_state",
    "ScoreRecord",
    "ScoreRunSummary",
    "ScoreRepository",
    "ScoreEngine",
    "ScoreService",
    "ScoreSettings",
    "ScoreHistory",
]
