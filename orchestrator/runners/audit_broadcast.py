from __future__ import annotations

import asyncio
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional
from uuid import UUID

from orchestrator.clients.ss58_client import SS58Client
from orchestrator.common.job_store import JobStatus, JobType
from orchestrator.common.payload_templates import build_img_h100_pcie_payload
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.domain.miner import Miner
from orchestrator.services.job_service import JobService, JobWaitTimeoutError
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.services.score_service import ScoreRecord, ScoreService
from orchestrator.services.listen_service import ListenService
from orchestrator.runners.base import InstrumentedRunner


_REFERENCE_IMAGE_URL = "https://media.dippy-bittensor.studio/callbacks/00013c92-7abf-441e-bf8c-ea559970534d.png"
_DEFAULT_TIMEOUT_SECONDS = 30.0
_DEFAULT_DISPATCH_CONCURRENCY = 8


@dataclass
class AuditBroadcastSummary:
    timestamp: datetime
    audit_job_id: UUID | None
    audit_hash: str | None
    miners_considered: int
    dispatched: int
    completed: int
    failures: int
    mismatches: int
    job_ids: Mapping[str, UUID]


class AuditBroadcastRunner(InstrumentedRunner[AuditBroadcastSummary]):
    """Dispatch a reference img-h100_pcie job to all scored miners and slash on mismatch."""

    def __init__(
        self,
        *,
        job_service: JobService,
        miner_metagraph_service: MinerMetagraphService,
        score_service: ScoreService,
        listen_service: ListenService,
        audit_miner: Miner,
        netuid: int,
        network: str,
        ss58_client: SS58Client | None = None,
        logger: StructuredLogger | logging.Logger | None = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        dispatch_concurrency: int = _DEFAULT_DISPATCH_CONCURRENCY,
    ) -> None:
        super().__init__(name="audit_broadcast.run", logger=logger)
        self._job_service = job_service
        self._miner_metagraph_service = miner_metagraph_service
        self._score_service = score_service
        self._listen_service = listen_service
        self._audit_miner = audit_miner
        self._netuid = netuid
        self._network = network
        self._ss58_client = ss58_client
        self._timeout_seconds = max(1.0, float(timeout_seconds))
        self._dispatch_concurrency = max(1, int(dispatch_concurrency))

    async def _run(self) -> AuditBroadcastSummary | None:
        miners = self._eligible_miners()
        if not miners:
            return self._build_summary(
                audit_job_id=None,
                audit_hash=None,
                miners_considered=0,
                dispatched=0,
                completed=0,
                failures=0,
                mismatches=0,
                job_ids={},
            )

        prompt = secrets.token_hex(16)
        seed = secrets.randbelow(2**32)
        base_payload = build_img_h100_pcie_payload(
            prompt=prompt,
            image_url=_REFERENCE_IMAGE_URL,
            seed=seed,
        )

        audit_job = await self._listen_service._create_job(
            job_type=JobType.FLUX_KONTEXT,
            payload=base_payload,
            miner=self._audit_miner,
            desired_job_id=None,
        )
        audit_dispatch_payload = self._listen_service._build_dispatch_payload(audit_job)
        audit_url = self._listen_service._resolve_inference_url(self._audit_miner, JobType.FLUX_KONTEXT)
        dispatched = await self._listen_service._dispatch(
            audit_job,
            self._audit_miner,
            audit_url,
            audit_dispatch_payload,
        )
        if not dispatched:
            return None

        try:
            audit_job = await self._job_service.wait_for_terminal_state(
                audit_job.job_id,
                timeout_seconds=self._timeout_seconds,
                poll_interval_seconds=1.0,
            )
        except JobWaitTimeoutError:
            self._logger.error(
                "audit_broadcast.audit_timeout",
                job_id=str(audit_job.job_id),
                timeout_seconds=self._timeout_seconds,
            )
            return None

        audit_hash = self._extract_image_hash(audit_job)
        job_records: dict[str, UUID] = {"audit": audit_job.job_id}

        dispatch_results = await self._dispatch_to_miners(
            miners,
            payload=base_payload,
        )
        job_records.update(dispatch_results["job_ids"])

        await asyncio.sleep(self._timeout_seconds)

        completed, failures, mismatches = await self._evaluate_jobs(
            dispatch_results["jobs"],
            audit_hash,
        )

        return self._build_summary(
            audit_job_id=audit_job.job_id,
            audit_hash=audit_hash,
            miners_considered=len(miners),
            dispatched=len(dispatch_results["jobs"]),
            completed=completed,
            failures=failures,
            mismatches=mismatches,
            job_ids=job_records,
        )

    def _start_fields(self) -> dict[str, Any]:
        return {"netuid": self._netuid, "network": self._network}

    def _eligible_miners(self) -> list[Miner]:
        miners = self._miner_metagraph_service.fetch_miners()
        if not miners:
            return []

        scores = self._score_service.all()
        eligible: list[Miner] = []
        for hotkey, miner in miners.items():
            record = scores.get(hotkey)
            if record is None:
                continue
            try:
                if float(getattr(record, "scores", 0.0)) <= 0.0:
                    continue
            except (TypeError, ValueError):
                continue
            eligible.append(miner)
        return eligible

    async def _dispatch_to_miners(
        self,
        miners: Iterable[Miner],
        *,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(self._dispatch_concurrency)
        jobs: list[tuple[str, UUID]] = []

        async def _send(miner: Miner) -> None:
            async with semaphore:
                job = await self._listen_service._create_job(
                    job_type=JobType.FLUX_KONTEXT,
                    payload=payload,
                    miner=miner,
                    desired_job_id=None,
                )
                dispatch_payload = self._listen_service._build_dispatch_payload(job)
                inference_url = self._listen_service._resolve_inference_url(miner, JobType.FLUX_KONTEXT)
                dispatched = await self._listen_service._dispatch(job, miner, inference_url, dispatch_payload)
                if dispatched:
                    jobs.append((miner.hotkey, job.job_id))

        await asyncio.gather(*[_send(miner) for miner in miners])

        return {
            "jobs": jobs,
            "job_ids": {hotkey: job_id for hotkey, job_id in jobs},
        }

    async def _evaluate_jobs(
        self,
        jobs: list[tuple[str, UUID]],
        audit_hash: str | None,
    ) -> tuple[int, int, int]:
        completed = 0
        failures = 0
        mismatches = 0

        for hotkey, job_id in jobs:
            try:
                job = await self._job_service.get_job(job_id)
            except Exception as exc:  # pragma: no cover - defensive guard
                self._logger.error(
                    "audit_broadcast.job_fetch_failed",
                    job_id=str(job_id),
                    hotkey=hotkey,
                    error=str(exc),
                )
                await self._slash_and_zero(hotkey)
                failures += 1
                continue

            if job.status != JobStatus.SUCCESS:
                await self._slash_and_zero(hotkey)
                failures += 1
                continue

            miner_hash = self._extract_image_hash(job)
            if not miner_hash or not audit_hash or miner_hash != audit_hash:
                mismatches += 1
                await self._slash_and_zero(hotkey)
            else:
                completed += 1

        return completed, failures, mismatches

    async def _slash_and_zero(self, hotkey: str) -> None:
        miner = self._miner_metagraph_service.get_miner(hotkey)
        if miner is not None:
            failed_audits = getattr(miner, "failed_audits", 0) or 0
            updated = miner.model_copy(update={"failed_audits": failed_audits + 1, "valid": False})
            try:
                self._miner_metagraph_service.upsert_miner(updated)
            except Exception as exc:  # pragma: no cover - defensive guard
                self._logger.debug("audit_broadcast.miner_update_failed", hotkey=hotkey, error=str(exc))

        existing_record = self._score_service.get(hotkey)
        failure_count = 0
        if existing_record is not None:
            try:
                failure_count = int(getattr(existing_record, "failure_count", 0))
            except (TypeError, ValueError):
                failure_count = 0
        try:
            self._score_service.put(hotkey, ScoreRecord(scores=0.0, failure_count=failure_count))
        except Exception as exc:  # pragma: no cover - defensive guard
            self._logger.debug("audit_broadcast.score_update_failed", hotkey=hotkey, error=str(exc))
        await self._record_ban(hotkey)

    async def _record_ban(self, hotkey: str) -> None:
        client = getattr(self, "_ss58_client", None)
        candidate = (hotkey or "").strip()
        if client is None or not candidate:
            return
        try:
            await client.append_addresses([candidate])
        except Exception as exc:  # pragma: no cover - defensive guard
            self._logger.debug("audit_broadcast.ss58_append_failed", hotkey=candidate, error=str(exc))

    @staticmethod
    def _extract_image_hash(job: Any) -> Optional[str]:
        response_payload = getattr(getattr(job, "job_response", None), "payload", None)
        if isinstance(response_payload, Mapping):
            candidate = response_payload.get("image_sha256") or response_payload.get("image_hash")
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip().lower()
        result_hash = getattr(job, "result_image_sha256", None)
        if isinstance(result_hash, str) and result_hash.strip():
            return result_hash.strip().lower()
        return None

    def _build_summary(
        self,
        *,
        audit_job_id: UUID | None,
        audit_hash: str | None,
        miners_considered: int,
        dispatched: int,
        completed: int,
        failures: int,
        mismatches: int,
        job_ids: Mapping[str, UUID],
    ) -> AuditBroadcastSummary:
        return AuditBroadcastSummary(
            timestamp=datetime.now(timezone.utc),
            audit_job_id=audit_job_id,
            audit_hash=audit_hash,
            miners_considered=miners_considered,
            dispatched=dispatched,
            completed=completed,
            failures=failures,
            mismatches=mismatches,
            job_ids=job_ids,
        )


__all__ = ["AuditBroadcastRunner", "AuditBroadcastSummary"]
