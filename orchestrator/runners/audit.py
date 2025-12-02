from __future__ import annotations

import logging
import secrets
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from urllib import error as urllib_error

from sn_uuid import uuid7

from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.common.datetime import parse_datetime
from orchestrator.common.job_store import AuditStatus, Job, JobRequest, JobStatus, JobType
from orchestrator.common.stubbing import AUDIT_MINER
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.domain.miner import Miner
from orchestrator.repositories import AuditFailureRecord, AuditFailureRepository
from orchestrator.services.audit_service import AuditRunSummary, AuditService
from orchestrator.services.listen_service import ListenService, _TASK_TYPE_PAYLOAD_OVERRIDES
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
        callback_url: str | None = None,
        limit: int = 1,
        preview_only: bool = False,
        job_type: str | None = "img-h100_pcie",
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
        self._callback_url = callback_url.strip() if callback_url else None
        self._epistula_client = EpistulaClient(None)
        base_logger = self._logger
        listen_logger = base_logger if isinstance(base_logger, StructuredLogger) else StructuredLogger(name="orchestrator.audit.listen")
        self._listen_service = ListenService(
            job_service=job_service,
            metagraph=_FixedMetagraph(),
            logger=listen_logger,
            epistula_client=self._epistula_client,
            callback_url=self._callback_url,
        )
        self._limit = max(1, int(limit))
        self._preview_only = bool(preview_only)
        default_job_type = "img-h100_pcie"
        normalized_job_type = str(job_type).strip() if job_type else default_job_type
        self._job_type = normalized_job_type or default_job_type
        self._job_type_lower = self._job_type.lower()

    async def run_once(self) -> AuditRunSummary | None:  # type: ignore[override]
        self._log(
            "info",
            "audit.seed.run.start",
            netuid=self._netuid,
            network=self._network,
            limit=self._limit,
            job_type=self._job_type,
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
        for job in candidates:
            self._log_selected_job(job, preview=self._preview_only)
        if self._preview_only:
            summary = AuditRunSummary(
                timestamp=datetime.now(timezone.utc),
                jobs_examined=len(candidates),
                audit_candidates=len(candidates),
                miners_marked_valid=0,
                miners_marked_invalid=0,
                applied_changes=False,
            )
            self._log(
                "info",
                "audit.seed.preview.run.complete",
                netuid=self._netuid,
                network=self._network,
                examined=summary.jobs_examined,
                audit_jobs_created=0,
                preview=True,
            )
            return summary

        created = 0
        audit_miner = AUDIT_MINER
        for job in candidates:
            job_id = self._normalize_uuid(job.get("job_id"))
            if job_id is None:
                continue
            try:
                audit_job = await self.create_audit_job(job_id, job, auditor=audit_miner)
            except Exception as exc:  # pragma: no cover - seed resilience
                self._log(
                    "warning",
                    "audit.seed.create_failed",
                    netuid=self._netuid,
                    network=self._network,
                    job_id=str(job_id) if job_id else "<missing>",
                    auditor_hotkey=audit_miner.hotkey,
                    error=str(exc),
                )
                continue
            try:
                await self._dispatch_audit_job(audit_job, audit_miner)
            except Exception as exc:  # pragma: no cover - dispatch resilience
                self._log(
                    "warning",
                    "audit.seed.dispatch_failed",
                    netuid=self._netuid,
                    network=self._network,
                    job_id=str(audit_job.job_id),
                    auditor_hotkey=audit_miner.hotkey,
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

    def _log_selected_job(self, job: dict[str, Any], *, preview: bool) -> None:
        job_id = self._normalize_uuid(job.get("job_id")) or job.get("job_id")
        hotkey = str(job.get("miner_hotkey") or "")
        self._log(
            "info",
            "audit.seed.preview" if preview else "audit.seed.selected",
            netuid=self._netuid,
            network=self._network,
            job_id=str(job_id),
            hotkey=hotkey,
            audit_target=str(job_id),
            job_details=self._summarize_job_for_logging(job),
        )

    def _select_candidates(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not jobs:
            return []

        required_job_type = self._job_type_lower
        audited_targets = {
            self._normalize_uuid(job.get("audit_target_job_id"))
            for job in jobs
            if job.get("is_audit_job") is True
        }
        candidates: list[tuple[datetime, dict[str, Any]]] = []
        for job in jobs:
            if job.get("is_audit_job") is True:
                continue
            if required_job_type:
                job_type = str(job.get("job_type") or "").strip()
                if job_type.lower() != required_job_type:
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

    def _summarize_job_for_logging(self, job: dict[str, Any]) -> dict[str, Any]:
        try:
            sanitized = self._job_service._sanitize_job_record(job)  # type: ignore[attr-defined]
        except Exception:
            sanitized = deepcopy(job)
        return self._strip_prompts_and_secrets(sanitized)

    @classmethod
    def _strip_prompts_and_secrets(cls, value: Any, key: str | None = None) -> Any:
        key_lower = key.lower() if isinstance(key, str) else ""
        if isinstance(value, dict):
            cleaned: dict[str, Any] = {}
            for child_key, child_value in value.items():
                child_key_lower = child_key.lower() if isinstance(child_key, str) else ""
                if child_key_lower == "callback_secret":
                    continue
                if "prompt" in child_key_lower and "seed" not in child_key_lower:
                    continue
                cleaned[child_key] = cls._strip_prompts_and_secrets(child_value, key=child_key)
            return cleaned

        if isinstance(value, list):
            return [cls._strip_prompts_and_secrets(item, key=key) for item in value]

        return value

    async def create_audit_job(
        self,
        target_job_id: uuid.UUID,
        job: dict[str, Any],
        *,
        auditor: Miner,
    ) -> Job:
        payload = deepcopy(job.get("payload") or {})
        callback_secret = secrets.token_hex(32)
        payload["callback_secret"] = callback_secret

        prompt_seed = self._resolve_seed(payload, job.get("prompt_seed"))
        payload["seed"] = prompt_seed

        job_type_value = str(job.get("job_type") or JobType.GENERATE.value)
        job_type = self._normalize_job_type(job_type_value)

        now = datetime.now(timezone.utc)
        iso_now = now.isoformat()

        relay_payload = {
            "job_type": job_type_value,
            "miner_hotkey": str(auditor.hotkey),
            "payload": payload,
            "creation_timestamp": iso_now,
            "last_updated_at": iso_now,
            "status": JobStatus.PENDING.value,
            "audit_status": AuditStatus.NOT_AUDITED.value,
            "verification_status": "nonverified",
            "is_audit_job": True,
            "audit_target_job_id": str(target_job_id),
            "callback_secret": callback_secret,
            "prompt_seed": prompt_seed,
        }

        audit_job_id = uuid7()
        await self._job_relay.create_job(audit_job_id, relay_payload)

        job_request = JobRequest(
            job_type=job_type,
            payload=deepcopy(payload),
            timestamp=now.timestamp(),
        )
        return Job(
            job_request=job_request,
            hotkey=str(auditor.hotkey),
            job_id=audit_job_id,
            status=JobStatus.PENDING,
            callback_secret=callback_secret,
            prompt_seed=prompt_seed,
            is_audit_job=True,
            audit_status=AuditStatus.NOT_AUDITED,
            audit_id=target_job_id,
        )

    async def _dispatch_audit_job(self, job: Job, miner: Miner) -> None:
        await self._job_service.mark_job_prepared(job.job_id)

        payload = self._listen_service._build_dispatch_payload(job)
        job_type = self._normalize_job_type(getattr(job.job_request, "job_type", None))
        inference_url = self._listen_service._resolve_inference_url(miner, job_type)
        timeout = self._listen_service._resolve_dispatch_timeout(job.job_request)

        try:
            status_code, response_text = await self._epistula_client.post_signed_request(
                url=inference_url,
                payload=payload,
                miner_hotkey=miner.hotkey,
                timeout=timeout,
            )
        except urllib_error.URLError as exc:
            await self._job_service.mark_job_failure(job.job_id, f"dispatch_error:{exc.reason or type(exc).__name__}")
            self._log(
                "error",
                "audit.seed.dispatch_error",
                job_id=str(job.job_id),
                url=inference_url,
                auditor_hotkey=miner.hotkey,
                error=str(exc),
            )
            return
        except Exception as exc:  # noqa: BLE001
            await self._job_service.mark_job_failure(job.job_id, f"dispatch_error:{type(exc).__name__}")
            self._log(
                "error",
                "audit.seed.dispatch_error",
                job_id=str(job.job_id),
                url=inference_url,
                auditor_hotkey=miner.hotkey,
                error=str(exc),
            )
            return

        if status_code >= 400:
            await self._job_service.mark_job_failure(job.job_id, f"dispatch_http_{status_code}")
            self._log(
                "warning",
                "audit.seed.dispatch_failed",
                job_id=str(job.job_id),
                url=inference_url,
                auditor_hotkey=miner.hotkey,
                status_code=status_code,
                response_preview=response_text[:200],
            )
            return

        await self._job_service.mark_job_dispatched(job.job_id)
        self._log(
            "info",
            "audit.seed.dispatch_success",
            job_id=str(job.job_id),
            url=inference_url,
            auditor_hotkey=miner.hotkey,
            status_code=status_code,
        )

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

    @staticmethod
    def _normalize_job_type(job_type: Any) -> JobType:
        try:
            return JobType(job_type)
        except Exception:  # noqa: BLE001
            return JobType.GENERATE


class _FixedMetagraph:
    """Minimal metagraph shim that always returns the configured audit miner."""

    def fetch_candidate(self, task_type: str | None = None) -> Miner | None:  # noqa: ARG002 - signature parity
        return AUDIT_MINER



class AuditCheckRunner(_BaseAuditRunner):
    """Runs the audit verification pass (optionally persists changes)."""

    def __init__(
        self,
        *,
        audit_service: AuditService,
        netuid: int,
        network: str,
        apply_changes: bool = True,
        audit_failure_repository: AuditFailureRepository | None = None,
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
        self._audit_failure_repo = audit_failure_repository

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

            self._record_failure(
                audit_job=audit_job,
                target_job=target_job,
                audit_hash=audit_hash,
                target_hash=target_hash,
                hotkey=hotkey or None,
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

    def _record_failure(
        self,
        *,
        audit_job: dict[str, Any],
        target_job: dict[str, Any],
        audit_hash: str,
        target_hash: str,
        hotkey: str | None,
    ) -> None:
        if self._audit_failure_repo is None:
            return

        audit_job_id = self._normalize_uuid(audit_job.get("job_id"))
        if audit_job_id is None:
            return

        try:
            record = AuditFailureRecord(
                id=uuid7(),
                created_at=datetime.now(timezone.utc),
                audit_job_id=audit_job_id,
                target_job_id=self._normalize_uuid(target_job.get("job_id")),
                miner_hotkey=hotkey.strip() if hotkey else None,
                netuid=self._netuid,
                network=self._network,
                audit_payload=self._extract_payload(audit_job),
                audit_response_payload=self._extract_response_payload(audit_job),
                target_payload=self._extract_payload(target_job),
                target_response_payload=self._extract_response_payload(target_job),
                audit_image_hash=audit_hash,
                target_image_hash=target_hash,
            )
            self._audit_failure_repo.upsert_failure(record)
        except Exception as exc:  # pragma: no cover - defensive persistence guard
            self._log(
                "warning",
                "audit.check.record_failure_failed",
                audit_job_id=str(audit_job_id),
                error=str(exc),
            )

    @staticmethod
    def _extract_payload(job: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(job, dict):
            return None
        payload = deepcopy(job.get("payload"))
        if not isinstance(payload, dict):
            return None
        payload.pop("callback_secret", None)
        return payload

    @staticmethod
    def _extract_response_payload(job: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(job, dict):
            return None
        payload = deepcopy(job.get("response_payload"))
        if not isinstance(payload, dict):
            return None
        payload.pop("callback_secret", None)
        return payload


__all__ = ["AuditSeedRunner", "AuditCheckRunner"]
