from __future__ import annotations

import logging
import uuid
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from orchestrator.common.job_store import JobType
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.domain.miner import Miner
from orchestrator.runners.base import InstrumentedRunner
from orchestrator.schemas.job import AuditStatus, JobRecord
from orchestrator.services.job_service import JobService
from orchestrator.services.listen_service import ListenService
from orchestrator.services.miner_metagraph_service import MinerMetagraphService


@dataclass
class SeedRequestsSummary:
    source_job_id: uuid.UUID
    source_job_type: str
    source_job_hotkey: str | None
    target_miners: int
    dispatched_job_ids: list[uuid.UUID]


class SeedRequestsRunner(InstrumentedRunner[SeedRequestsSummary]):
    """Seed requests by replaying the latest audit job to all eligible miners."""

    def __init__(
        self,
        *,
        job_service: JobService,
        miner_metagraph_service: MinerMetagraphService,
        listen_service: ListenService,
        netuid: int,
        network: str,
        job_lookup_limit: int = 200,
        logger: StructuredLogger | logging.Logger | None = None,
    ) -> None:
        self._job_service = job_service
        self._miner_metagraph_service = miner_metagraph_service
        self._listen_service = listen_service
        self._netuid = netuid
        self._network = network
        self._job_lookup_limit = max(1, int(job_lookup_limit or 0))
        super().__init__(name="seed.requests.run", logger=logger)

    async def _run(self) -> SeedRequestsSummary | None:
        audit_job = await self._fetch_latest_audit_job()
        if audit_job is None:
            self._log(
                "info",
                "seed.requests.no_audit_jobs",
                netuid=self._netuid,
                network=self._network,
            )
            return None

        job_type = self._normalize_job_type(audit_job.job_type)
        if job_type is None:
            self._log(
                "warning",
                "seed.requests.unsupported_job_type",
                job_type=str(getattr(audit_job, "job_type", "")),
                job_id=str(audit_job.job_id),
            )
            return None

        payload = getattr(audit_job, "payload", None)
        if not isinstance(payload, dict):
            self._log(
                "warning",
                "seed.requests.invalid_payload",
                job_id=str(audit_job.job_id),
                payload_type=type(payload).__name__,
            )
            return None

        targets = self._eligible_miners(job_type)
        if not targets:
            self._log(
                "info",
                "seed.requests.no_targets",
                job_type=str(job_type.value if isinstance(job_type, JobType) else job_type),
            )
            return SeedRequestsSummary(
                source_job_id=audit_job.job_id,
                source_job_type=str(job_type),
                source_job_hotkey=getattr(audit_job, "miner_hotkey", None),
                target_miners=0,
                dispatched_job_ids=[],
            )

        dispatched: list[uuid.UUID] = []
        for miner in targets:
            job_id = await self._dispatch_to_miner(
                miner=miner,
                job_type=job_type,
                payload=payload,
            )
            if job_id is not None:
                dispatched.append(job_id)

        return SeedRequestsSummary(
            source_job_id=audit_job.job_id,
            source_job_type=str(job_type),
            source_job_hotkey=getattr(audit_job, "miner_hotkey", None),
            target_miners=len(targets),
            dispatched_job_ids=dispatched,
        )

    def _start_fields(self) -> dict[str, Any]:
        return {"netuid": self._netuid, "network": self._network}

    def _complete_fields(self, result: SeedRequestsSummary | None, start_fields: dict[str, Any]) -> dict[str, Any]:
        if result is None:
            return {**start_fields, "status": "skipped"}
        return {
            **start_fields,
            "status": "success",
            "source_job_id": str(result.source_job_id),
            "source_job_type": result.source_job_type,
            "source_job_hotkey": result.source_job_hotkey,
            "target_miners": result.target_miners,
            "dispatched_jobs": len(result.dispatched_job_ids),
            "dispatch_job_ids": [str(job_id) for job_id in result.dispatched_job_ids],
        }

    async def _fetch_latest_audit_job(self) -> JobRecord | None:
        records = await self._job_service.list_recent_completed_jobs(
            max_results=self._job_lookup_limit,
            lookback_days=None,
        )
        for record in records:
            if record.is_audit_job:
                return record
        return None

    def _eligible_miners(self, job_type: JobType | str) -> list[Miner]:
        state = self._miner_metagraph_service.dump_full_state()
        if not state:
            return []
        return [
            miner
            for miner in state.values()
            if self._supports_job_type(miner, job_type) and self._has_network_address(miner)
        ]

    @staticmethod
    def _supports_job_type(miner: Miner, job_type: JobType | str) -> bool:
        capability = job_type.value if isinstance(job_type, JobType) else str(job_type)
        if not capability:
            return False

        capacity = getattr(miner, "capacity", {}) or {}
        if not isinstance(capacity, dict):
            return False

        try:
            if bool(capacity.get(capability)):
                return True
        except Exception:
            pass

        inference_caps = capacity.get("inference")
        if isinstance(inference_caps, (list, tuple, set)):
            normalized = {str(item).strip().lower() for item in inference_caps if item is not None}
            if capability.strip().lower() in normalized:
                return True

        return False

    @staticmethod
    def _has_network_address(miner: Miner) -> bool:
        address = getattr(miner, "network_address", None)
        return isinstance(address, str) and bool(address.strip())

    async def _dispatch_to_miner(
        self,
        *,
        miner: Miner,
        job_type: JobType | str,
        payload: dict[str, Any],
    ) -> uuid.UUID | None:
        try:
            job_id = await self._listen_service.process(
                job_type=job_type,
                payload=deepcopy(payload),
                desired_job_id=None,
                override_miner=miner,
            )
            self._log(
                "info",
                "seed.requests.dispatched",
                job_id=str(job_id),
                job_type=str(job_type),
                miner_hotkey=getattr(miner, "hotkey", None),
                miner_addr=getattr(miner, "network_address", None),
            )
            return uuid.UUID(str(job_id))
        except Exception as exc:  # noqa: BLE001
            self._log(
                "warning",
                "seed.requests.dispatch_failed",
                job_type=str(job_type),
                miner_hotkey=getattr(miner, "hotkey", None),
                error=str(exc),
            )
            return None

    @staticmethod
    def _normalize_job_type(value: Any) -> JobType | str | None:
        if value is None:
            return None
        if isinstance(value, JobType):
            return value
        try:
            return JobType(str(value))
        except Exception:
            text = str(value).strip()
            return text or None
