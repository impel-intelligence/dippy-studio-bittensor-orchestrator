from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

import httpx


logger = logging.getLogger(__name__)


class WebhookDispatcher:
    """Background dispatcher that forwards callback payloads to remote webhooks."""

    def __init__(
        self,
        *,
        max_workers: int = 4,
        max_attempts: int = 3,
        initial_backoff_seconds: float = 1.0,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="webhook-dispatcher",
        )
        self._max_attempts = max(1, int(max_attempts or 1))
        self._backoff = max(0.0, float(initial_backoff_seconds))
        self._timeout = max(1.0, float(timeout_seconds))

    def dispatch(
        self,
        *,
        webhook_url: str,
        job_id: str,
        status: str,
        completed_at: str,
        error: Optional[str],
        latencies: Dict[str, Any],
        image_url: Optional[str],
    ) -> bool:
        url = (webhook_url or "").strip()
        if not url:
            return False

        self._executor.submit(
            self._send_with_retries,
            url,
            str(job_id),
            status,
            completed_at,
            error,
            latencies or {},
            image_url,
        )
        return True

    def _send_with_retries(
        self,
        webhook_url: str,
        job_id: str,
        status: str,
        completed_at: str,
        error: Optional[str],
        latencies: Dict[str, Any],
        image_url: Optional[str],
    ) -> None:
        backoff = self._backoff
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._post_once(
                    webhook_url=webhook_url,
                    job_id=job_id,
                    status=status,
                    completed_at=completed_at,
                    error=error,
                    latencies=latencies,
                    image_url=image_url,
                )
                logger.info(
                    "webhook.dispatch.success job_id=%s url=%s attempt=%s",
                    job_id,
                    webhook_url,
                    attempt,
                )
                return
            except Exception as exc:  # noqa: BLE001 - defensive retry guard
                logger.warning(
                    "webhook.dispatch.failed job_id=%s url=%s attempt=%s error=%s",
                    job_id,
                    webhook_url,
                    attempt,
                    str(exc),
                )
                if attempt >= self._max_attempts:
                    logger.error(
                        "webhook.dispatch.exhausted job_id=%s url=%s attempts=%s",
                        job_id,
                        webhook_url,
                        self._max_attempts,
                    )
                    return
                time.sleep(backoff)
                backoff = backoff * 2 if backoff > 0 else 0

    def _post_once(
        self,
        *,
        webhook_url: str,
        job_id: str,
        status: str,
        completed_at: str,
        error: Optional[str],
        latencies: Dict[str, Any],
        image_url: Optional[str],
    ) -> None:
        data: Dict[str, Any] = {
            "job_id": job_id,
            "status": status,
            "completed_at": completed_at,
        }
        if error:
            data["error"] = error
        for key, value in (latencies or {}).items():
            if value is None:
                continue
            data[key] = value

        if image_url:
            data["image_url"] = image_url

        response = httpx.post(
            webhook_url,
            json=data,
            timeout=self._timeout,
        )
        response.raise_for_status()


__all__ = ["WebhookDispatcher"]
