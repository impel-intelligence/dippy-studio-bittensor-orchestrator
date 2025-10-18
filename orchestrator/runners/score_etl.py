from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.services.job_scoring import job_to_score
from orchestrator.services.score_service import ScoreRecord, ScoreService


class ScoreETLRunner:
    """Periodically recompute miner scores from recent inference jobs."""

    _COMPLETED_STATUSES = {"success"}
    _LOOKBACK_WINDOW = timedelta(days=7)

    def __init__(
        self,
        *,
        miner_metagraph_client: LiveMinerMetagraphClient,
        job_relay_client: BaseJobRelayClient,
        score_service: ScoreService,
        netuid: int,
        network: str,
        interval_seconds: float = 3600.0,
        logger: StructuredLogger | logging.Logger | None = None,
        score_fn: Callable[[Mapping[str, Any]], float] = job_to_score,
    ) -> None:
        self._miner_metagraph_client = miner_metagraph_client
        self._job_relay_client = job_relay_client
        self._score_service = score_service
        self._netuid = netuid
        self._network = network
        self._interval_seconds = max(interval_seconds, 0.0)
        self._score_fn = score_fn
        self._logger: StructuredLogger | logging.Logger = (
            logger if logger is not None else logging.getLogger(__name__)
        )
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._trigger_event: Optional[asyncio.Event] = None

    async def start(self) -> None:
        if self._task is not None:
            return

        self._stop_event = asyncio.Event()
        self._trigger_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._run())

    async def stop(self) -> None:
        if self._task is None or self._stop_event is None:
            return

        self._stop_event.set()
        if self._trigger_event is not None:
            self._trigger_event.set()
        try:
            await self._task
        finally:
            self._task = None
            self._stop_event = None
            self._trigger_event = None

    async def run_once(self) -> None:
        await self._sync()

    async def _run(self) -> None:
        assert self._stop_event is not None
        assert self._trigger_event is not None

        while not self._stop_event.is_set():
            await self._sync()
            if self._interval_seconds == 0:
                await asyncio.sleep(0)
                continue
            try:
                await asyncio.wait_for(
                    self._trigger_event.wait(),
                    timeout=self._interval_seconds,
                )
            except asyncio.TimeoutError:
                continue
            finally:
                if self._trigger_event.is_set():
                    self._trigger_event.clear()

    async def _sync(self) -> None:
        
        self._log(
            "info",
            "score_etl.sync.disabled",
            netuid=self._netuid,
            network=self._network,
        )
        return

    def trigger(self) -> None:
        """Request an immediate scoring pass without waiting for the interval."""
        if self._trigger_event is None:
            self._log("debug", "score_etl.trigger.skipped_not_running")
            return

        self._trigger_event.set()
        self._log("debug", "score_etl.trigger.queued")

    @classmethod
    def _is_completed_inference_job(
        cls, job: Mapping[str, Any], cutoff: datetime
    ) -> bool:
        job_type = str(job.get("job_type") or "").lower()
        if job_type != "inference":
            return False

        if job.get("is_audit_job") is True:
            return False

        status = str(job.get("status") or "").lower()
        if status not in cls._COMPLETED_STATUSES:
            return False

        completed_at = cls._parse_datetime(job.get("completed_at"))
        if completed_at is None or completed_at < cutoff:
            return False

        return True

    @staticmethod
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

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = self._logger
        if isinstance(logger, StructuredLogger):
            log_method = getattr(logger, level)
            log_method(event, **fields)
            return

        log_method = getattr(logger, level)
        if fields:
            log_method("%s %s", event, fields)
        else:
            log_method(event)
