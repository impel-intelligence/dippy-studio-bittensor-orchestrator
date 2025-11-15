from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence

from orchestrator.domain.miner import Miner
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.schemas.job import AuditStatus, JobRecord
from orchestrator.services.job_service import JobService

LOGGER = logging.getLogger(__name__)


@dataclass
class AuditRunSummary:
    timestamp: datetime
    jobs_examined: int
    audit_candidates: int
    miners_marked_valid: int
    miners_marked_invalid: int
    applied_changes: bool


class AuditService:
    """Coordinates miner audit sampling and validity updates."""

    def __init__(
        self,
        *,
        job_service: JobService,
        miner_metagraph_service: MinerMetagraphService,
        audit_sample_size: float = 0.1,
        batch_limit: int = 100,
        logger: logging.Logger | None = None,
    ) -> None:
        self._job_service = job_service
        self._miner_metagraph_service = miner_metagraph_service
        self._audit_sample_size = self._clamp_sample(audit_sample_size)
        self._batch_limit = max(1, int(batch_limit))
        self._logger = logger if logger is not None else LOGGER

    async def run_once(self, *, apply_changes: bool = False) -> AuditRunSummary:
        jobs = await self._job_service.dump_jobs(limit=self._batch_limit)
        sampled_jobs = self._select_jobs(jobs)
        decisions = list(self._iter_validity_decisions(sampled_jobs))

        miners_marked_valid = 0
        miners_marked_invalid = 0

        if apply_changes and decisions:
            for hotkey, desired_valid in decisions:
                try:
                    updated = self._apply_validity(hotkey, desired_valid)
                except Exception as exc:  # pragma: no cover - defensive logging
                    self._logger.warning(
                        "audit.apply_validity_failed hotkey=%s desired=%s error=%s",
                        hotkey,
                        desired_valid,
                        exc,
                    )
                    continue
                if not updated:
                    continue
                if desired_valid:
                    miners_marked_valid += 1
                else:
                    miners_marked_invalid += 1

        summary = AuditRunSummary(
            timestamp=datetime.now(timezone.utc),
            jobs_examined=len(sampled_jobs),
            audit_candidates=len(decisions),
            miners_marked_valid=miners_marked_valid,
            miners_marked_invalid=miners_marked_invalid,
            applied_changes=apply_changes,
        )
        return summary

    def _select_jobs(self, jobs: Sequence[JobRecord]) -> list[JobRecord]:
        if not jobs:
            return []
        if self._audit_sample_size <= 0.0:
            return list(jobs)
        sample_count = max(1, int(len(jobs) * self._audit_sample_size))
        sample_count = min(sample_count, len(jobs))
        return list(jobs[:sample_count])

    def _iter_validity_decisions(
        self,
        jobs: Sequence[JobRecord],
    ) -> Iterable[tuple[str, bool]]:
        for job in jobs:
            decision = self._decide_validity(job)
            if decision is not None:
                yield decision

    def _decide_validity(self, job: JobRecord) -> tuple[str, bool] | None:
        if not job.is_audit_job:
            return None
        if job.audit_status == AuditStatus.audit_success:
            return job.miner_hotkey, True
        if job.audit_status == AuditStatus.audit_failed:
            return job.miner_hotkey, False
        return None

    def _apply_validity(self, hotkey: str, is_valid: bool) -> bool:
        miner = self._miner_metagraph_service.get_miner(hotkey)
        if miner is None:
            return False

        if bool(miner.valid) == bool(is_valid):
            return False

        updated = self._clone_with_validity(miner, is_valid)
        self._miner_metagraph_service.upsert_miner(updated)
        return True

    @staticmethod
    def _clone_with_validity(source: Miner, valid: bool) -> Miner:
        if hasattr(source, "model_copy"):
            clone = source.model_copy(deep=True)  # type: ignore[attr-defined]
        elif hasattr(source, "copy"):
            clone = source.copy(deep=True)  # type: ignore[attr-defined]
        else:
            if hasattr(source, "model_dump"):
                data = source.model_dump()  # type: ignore[attr-defined]
            elif hasattr(source, "dict"):
                data = source.dict()  # type: ignore[attr-defined]
            else:
                data = dict(source)
            clone = Miner(**data)
        clone.valid = bool(valid)
        return clone

    @staticmethod
    def _clamp_sample(value: float) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0
        if numeric < 0.0:
            return 0.0
        if numeric > 1.0:
            return 1.0
        return numeric


__all__ = ["AuditRunSummary", "AuditService"]
