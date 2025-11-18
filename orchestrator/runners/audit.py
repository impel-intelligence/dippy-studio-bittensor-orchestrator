from __future__ import annotations

import logging
import secrets
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from orchestrator.common.datetime import parse_datetime
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.domain.miner import Miner
from orchestrator.services.audit_service import AuditRunSummary, AuditService
from orchestrator.services.job_service import JobService
from orchestrator.services.miner_metagraph_service import MinerMetagraphService


class _BaseAuditRunner:
    """Shared audit runner plumbing with structured logging."""

    def __init__(
        self,
        *,
        audit_service: AuditService,
        netuid: int,
        network: str,
        phase: str,
        apply_changes: bool,
        logger: StructuredLogger | logging.Logger | None = None,
    ) -> None:
        self._audit_service = audit_service
        self._netuid = netuid
        self._network = network
        self._phase = phase
        self._apply_changes = apply_changes
        self._logger: StructuredLogger | logging.Logger = (
            logger if logger is not None else logging.getLogger(__name__)
        )

    async def run_once(self) -> AuditRunSummary | None:
        self._log(
            "info",
            f"audit.{self._phase}.run.start",
            netuid=self._netuid,
            network=self._network,
            apply_changes=self._apply_changes,
        )
        try:
            summary = await self._audit_service.run_once(apply_changes=self._apply_changes)
        except Exception as exc:  # pragma: no cover - logging safeguard
            self._log(
                "error",
                f"audit.{self._phase}.run.failed",
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return None

        self._log(
            "info",
            f"audit.{self._phase}.run.complete",
            netuid=self._netuid,
            network=self._network,
            jobs_examined=summary.jobs_examined,
            audit_candidates=summary.audit_candidates,
            miners_marked_valid=summary.miners_marked_valid,
            miners_marked_invalid=summary.miners_marked_invalid,
            applied_changes=summary.applied_changes,
            completed_at=summary.timestamp.isoformat(),
        )
        return summary

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

    @staticmethod
    def _normalize_uuid(value: Any) -> uuid.UUID | None:
        if isinstance(value, uuid.UUID):
            return value
        if isinstance(value, str) and value:
            try:
                return uuid.UUID(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _is_completed(job: dict[str, Any]) -> bool:
        status = str(job.get("status") or "").lower()
        return status in {"success", "completed"}


class AuditSeedRunner(_BaseAuditRunner):
    """Seeds audit jobs by cloning recent completed inferences."""

    def __init__(
        self,
        *,
        audit_service: AuditService,
        netuid: int,
        network: str,
        limit: int = 10,
        logger: StructuredLogger | logging.Logger | None = None,
    ) -> None:
        super().__init__(
            audit_service=audit_service,
            netuid=netuid,
            network=network,
            phase="seed",
            apply_changes=False,
            logger=logger,
        )
        job_service = getattr(audit_service, "_job_service", None)
        if not isinstance(job_service, JobService):  # pragma: no cover - defensive in prod
            raise ValueError("AuditService must expose JobService for seeding")
        self._job_service: JobService = job_service
        self._job_relay = job_service.job_relay
        self._limit = max(1, int(limit))

    async def run_once(self) -> AuditRunSummary | None:  # type: ignore[override]
        self._log(
            "info",
            "audit.seed.run.start",
            netuid=self._netuid,
            network=self._network,
            limit=self._limit,
        )
        try:
            jobs = await self._job_relay.list_jobs()
        except Exception as exc:  # pragma: no cover - remote call safeguard
            self._log(
                "error",
                "audit.seed.fetch_failed",
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return None

        candidates = self._select_candidates(jobs)
        created = 0
        for job in candidates:
            job_id = self._normalize_uuid(job.get("job_id"))
            if job_id is None:
                continue
            try:
                await self._create_audit_job(job_id, job)
            except Exception as exc:  # pragma: no cover - seed resilience
                self._log(
                    "warning",
                    "audit.seed.create_failed",
                    netuid=self._netuid,
                    network=self._network,
                    job_id=str(job_id),
                    error=str(exc),
                )
                continue
            created += 1

        summary = AuditRunSummary(
            timestamp=datetime.now(timezone.utc),
            jobs_examined=len(candidates),
            audit_candidates=created,
            miners_marked_valid=0,
            miners_marked_invalid=0,
            applied_changes=False,
        )
        self._log(
            "info",
            "audit.seed.run.complete",
            netuid=self._netuid,
            network=self._network,
            examined=summary.jobs_examined,
            audit_jobs_created=summary.audit_candidates,
        )
        return summary

    def _select_candidates(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not jobs:
            return []

        audited_targets = {
            self._normalize_uuid(job.get("audit_target_job_id"))
            for job in jobs
            if job.get("is_audit_job") is True
        }
        candidates: list[tuple[datetime, dict[str, Any]]] = []
        for job in jobs:
            if job.get("is_audit_job") is True:
                continue
            if not self._is_completed(job):
                continue
            if not self._is_not_audited(job):
                continue
            original_id = self._normalize_uuid(job.get("job_id"))
            if original_id is None or original_id in audited_targets:
                continue
            completed_at = self._extract_timestamp(job)
            candidates.append((completed_at, job))

        candidates.sort(key=lambda item: item[0], reverse=True)
        return [job for _, job in candidates[: self._limit]]

    @staticmethod
    def _is_not_audited(job: dict[str, Any]) -> bool:
        return str(job.get("audit_status") or "").lower() in {"not_audited", "not-audited"}

    @staticmethod
    def _extract_timestamp(job: dict[str, Any]) -> datetime:
        for key in ("completed_at", "last_updated_at", "creation_timestamp"):
            value = job.get(key)
            parsed = parse_datetime(value)
            if parsed is not None:
                return parsed
        return datetime.fromtimestamp(0, tz=timezone.utc)

    async def _create_audit_job(self, target_job_id: uuid.UUID, job: dict[str, Any]) -> None:
        payload = deepcopy(job.get("payload") or {})
        callback_secret = secrets.token_hex(32)
        payload["callback_secret"] = callback_secret

        job_service = self._job_service
        prompt_seed = self._resolve_seed(payload, job.get("prompt_seed"))
        payload["seed"] = prompt_seed

        miner_hotkey = job.get("miner_hotkey")
        if not miner_hotkey:
            raise ValueError("audit seed job missing miner_hotkey")

        now = datetime.now(timezone.utc)
        iso_now = now.isoformat()

        relay_payload = {
            "job_type": str(job.get("job_type") or "generate"),
            "miner_hotkey": str(miner_hotkey),
            "payload": payload,
            "creation_timestamp": iso_now,
            "last_updated_at": iso_now,
            "status": "pending",
            "audit_status": "not_audited",
            "verification_status": "nonverified",
            "is_audit_job": True,
            "audit_target_job_id": str(target_job_id),
            "callback_secret": callback_secret,
            "prompt_seed": prompt_seed,
        }

        audit_job_id = uuid.uuid4()
        await job_service.job_relay.create_job(audit_job_id, relay_payload)

    @staticmethod
    def _resolve_seed(payload: dict[str, Any], prompt_seed: Any) -> int:
        if "seed" in payload and payload["seed"] is not None:
            try:
                return int(payload["seed"])
            except (TypeError, ValueError):
                payload.pop("seed", None)
        if prompt_seed is not None:
            try:
                return int(prompt_seed)
            except (TypeError, ValueError):
                pass
        return secrets.randbelow(2**32)



class AuditCheckRunner(_BaseAuditRunner):
    """Runs the audit verification pass (optionally persists changes)."""

    def __init__(
        self,
        *,
        audit_service: AuditService,
        netuid: int,
        network: str,
        apply_changes: bool = True,
        logger: StructuredLogger | logging.Logger | None = None,
    ) -> None:
        super().__init__(
            audit_service=audit_service,
            netuid=netuid,
            network=network,
            phase="check",
            apply_changes=apply_changes,
            logger=logger,
        )
        job_service = getattr(audit_service, "_job_service", None)
        miner_client = getattr(audit_service, "_miner_metagraph_service", None)
        if not isinstance(job_service, JobService):  # pragma: no cover - defensive guard
            raise ValueError("AuditService must expose JobService for audit checks")
        if not isinstance(miner_client, MinerMetagraphService):  # pragma: no cover - defensive guard
            raise ValueError("AuditService must expose MinerMetagraphService for audit checks")
        self._job_service: JobService = job_service
        self._job_relay = job_service.job_relay
        self._miner_client: MinerMetagraphService = miner_client

    async def run_once(self) -> AuditRunSummary | None:  # type: ignore[override]
        self._log(
            "info",
            "audit.check.run.start",
            netuid=self._netuid,
            network=self._network,
            apply_changes=self._apply_changes,
        )
        try:
            jobs = await self._job_relay.list_jobs()
        except Exception as exc:  # pragma: no cover - remote fetch guard
            self._log(
                "error",
                "audit.check.fetch_failed",
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return None

        job_index = self._index_jobs(jobs)
        audit_jobs = [job for job in jobs if job.get("is_audit_job") is True and self._is_completed(job)]

        mismatches = 0
        increments = 0

        for audit_job in audit_jobs:
            target_id = self._normalize_uuid(audit_job.get("audit_target_job_id"))
            if target_id is None:
                continue

            target_job = job_index.get(target_id)
            if target_job is None:
                try:
                    target_job = await self._job_relay.fetch_job(target_id)
                except Exception as exc:  # pragma: no cover - network guard
                    self._log(
                        "warning",
                        "audit.check.target_fetch_failed",
                        audit_job_id=str(audit_job.get("job_id")),
                        target_job_id=str(target_id),
                        error=str(exc),
                    )
                    continue
                if target_job is None:
                    continue
                job_index[target_id] = target_job

            audit_hash = self._extract_image_hash(audit_job)
            target_hash = self._extract_image_hash(target_job)
            if not audit_hash or not target_hash:
                self._log(
                    "warning",
                    "audit.check.hash_missing",
                    audit_job_id=str(audit_job.get("job_id")),
                    target_job_id=str(target_id),
                    audit_hash_present=bool(audit_hash),
                    target_hash_present=bool(target_hash),
                )
                continue

            if self._hash_equal(audit_hash, target_hash):
                continue

            mismatches += 1
            hotkey = str(target_job.get("miner_hotkey") or "").strip()
            if not hotkey:
                self._log(
                    "warning",
                    "audit.check.no_hotkey",
                    target_job_id=str(target_id),
                )
                continue

            self._log(
                "info",
                "audit.check.hash_mismatch",
                audit_job_id=str(audit_job.get("job_id")),
                target_job_id=str(target_id),
                hotkey=hotkey,
            )

            if not self._apply_changes:
                continue

            updated = self._increment_failed_audits(hotkey)
            if updated is None:
                self._log(
                    "warning",
                    "audit.check.miner_missing",
                    hotkey=hotkey,
                )
                continue

            increments += 1
            self._log(
                "info",
                "audit.check.failed_audits_incremented",
                hotkey=hotkey,
                failed_audits=updated.failed_audits,
            )

        summary = AuditRunSummary(
            timestamp=datetime.now(timezone.utc),
            jobs_examined=len(audit_jobs),
            audit_candidates=mismatches,
            miners_marked_valid=0,
            miners_marked_invalid=increments if self._apply_changes else 0,
            applied_changes=self._apply_changes,
        )
        self._log(
            "info",
            "audit.check.run.complete",
            netuid=self._netuid,
            network=self._network,
            examined=len(audit_jobs),
            mismatches=mismatches,
            increments=increments,
            applied=self._apply_changes,
        )
        return summary

    def _index_jobs(self, jobs: list[dict[str, Any]]) -> dict[uuid.UUID, dict[str, Any]]:
        index: dict[uuid.UUID, dict[str, Any]] = {}
        for job in jobs:
            job_id = self._normalize_uuid(job.get("job_id"))
            if job_id is not None:
                index[job_id] = job
        return index

    @staticmethod
    def _extract_image_hash(job: dict[str, Any]) -> str | None:
        response = job.get("response_payload")
        hash_value: Any = None
        if isinstance(response, dict):
            hash_value = response.get("image_sha256") or response.get("image_hash")
        if not hash_value:
            hash_value = job.get("result_image_sha256")
        if isinstance(hash_value, str):
            candidate = hash_value.strip()
            if candidate:
                return candidate.lower()
        return None

    @staticmethod
    def _hash_equal(lhs: str, rhs: str) -> bool:
        return lhs.strip().lower() == rhs.strip().lower()

    def _increment_failed_audits(self, hotkey: str) -> Miner | None:
        miner = self._miner_client.get_miner(hotkey)
        if miner is None:
            return None
        failed_audits = getattr(miner, "failed_audits", 0) or 0
        updated = miner.model_copy(update={"failed_audits": failed_audits + 1})
        return self._miner_client.upsert_miner(updated)


__all__ = ["AuditSeedRunner", "AuditCheckRunner"]
