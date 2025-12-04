from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from orchestrator.common.job_store import JobStatus
from orchestrator.services.job_service import JobWaitCancelledError, JobWaitTimeoutError

try:  # Optional dependency – only needed when Redis backend is enabled
    from redis.asyncio import Redis  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dependency guard
    Redis = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


@dataclass
class SyncCallbackResult:
    """Payload returned to synchronous listeners once a callback arrives."""

    job_id: uuid.UUID
    status: JobStatus
    payload: Dict[str, Any] | None = None
    image_bytes: bytes | None = None
    content_type: str | None = None
    failure_reason: str | None = None
    error: str | None = None


class BaseSyncCallbackWaiter:
    """Interface for waiting on miner callback results."""

    async def register(self, job_id: uuid.UUID | str) -> None:
        raise NotImplementedError

    async def resolve(self, result: SyncCallbackResult) -> None:
        raise NotImplementedError

    async def fail(
        self,
        job_id: uuid.UUID | str,
        *,
        status: JobStatus = JobStatus.FAILED,
        error: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        raise NotImplementedError

    async def wait_for_result(
        self,
        job_id: uuid.UUID,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.1,
        is_disconnected: Callable[[], Awaitable[bool]] | None = None,
    ) -> SyncCallbackResult:
        raise NotImplementedError


class InMemorySyncCallbackWaiter(BaseSyncCallbackWaiter):
    """Track in-flight jobs using per-process futures."""

    def __init__(self) -> None:
        self._waiters: dict[str, asyncio.Future[SyncCallbackResult]] = {}

    async def register(self, job_id: uuid.UUID | str) -> None:
        key = str(job_id)
        existing = self._waiters.get(key)
        if existing is not None:
            return
        loop = asyncio.get_running_loop()
        self._waiters[key] = loop.create_future()

    async def resolve(self, result: SyncCallbackResult) -> None:
        key = str(result.job_id)
        future = self._waiters.get(key)
        if future is None:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._waiters[key] = future
        if not future.done():
            future.set_result(result)

    async def fail(
        self,
        job_id: uuid.UUID | str,
        *,
        status: JobStatus = JobStatus.FAILED,
        error: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        key = str(job_id)
        future = self._waiters.get(key)
        if future is None:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._waiters[key] = future
        if future.done():
            return
        result = SyncCallbackResult(
            job_id=self._coerce_uuid(job_id),
            status=status,
            payload=None,
            image_bytes=None,
            content_type=None,
            failure_reason=failure_reason,
            error=error,
        )
        future.set_result(result)

    async def wait_for_result(
        self,
        job_id: uuid.UUID,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.1,
        is_disconnected: Callable[[], Awaitable[bool]] | None = None,
    ) -> SyncCallbackResult:
        key = str(job_id)
        await self.register(job_id)
        future = self._waiters.get(key)
        if future is None:
            raise RuntimeError("Failed to register sync callback waiter")

        timeout = max(float(timeout_seconds), 0.0)
        poll = max(float(poll_interval_seconds), 0.05)
        is_infinite = timeout == float("inf")
        deadline: float | None = None if is_infinite else time.monotonic() + timeout

        try:
            while True:
                if is_disconnected is not None and await is_disconnected():
                    raise JobWaitCancelledError("Client disconnected while waiting for callback")

                if future.done():
                    if future.cancelled():
                        raise JobWaitCancelledError("Sync wait was cancelled")
                    return future.result()

                if not is_infinite and deadline is not None:
                    now = time.monotonic()
                    if now >= deadline:
                        raise JobWaitTimeoutError(f"Job {job_id} did not complete within {timeout} seconds")
                    remaining = deadline - now
                    wait_for = min(poll, max(remaining, 0.0))
                else:
                    wait_for = poll

                try:
                    return await asyncio.wait_for(future, timeout=wait_for)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    raise JobWaitCancelledError("Sync wait was cancelled")
        finally:
            self._waiters.pop(key, None)

    @staticmethod
    def _coerce_uuid(value: uuid.UUID | str) -> uuid.UUID:
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except Exception:  # pragma: no cover - defensive
            return uuid.uuid5(uuid.NAMESPACE_DNS, str(value))


class RedisSyncCallbackWaiter(BaseSyncCallbackWaiter):
    """Redis-backed waiter that supports multi-instance orchestration."""

    def __init__(
        self,
        redis_client: Any,
        *,
        channel_prefix: str = "listen_sync",
        result_ttl_seconds: int = 600,
    ) -> None:
        if Redis is None:
            raise RuntimeError("redis-py is required for Redis sync waiter")
        if redis_client is None:
            raise ValueError("Redis client is required for Redis sync waiter")
        self._redis: Redis = redis_client
        self._prefix = channel_prefix.strip() or "listen_sync"
        self._ttl = max(int(result_ttl_seconds or 0), 1)
        self._local = InMemorySyncCallbackWaiter()

    async def register(self, job_id: uuid.UUID | str) -> None:
        await self._local.register(job_id)

    async def resolve(self, result: SyncCallbackResult) -> None:
        await self._local.resolve(result)
        await self._store_result(result)
        await self._publish_result(result)

    async def fail(
        self,
        job_id: uuid.UUID | str,
        *,
        status: JobStatus = JobStatus.FAILED,
        error: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        result = SyncCallbackResult(
            job_id=self._coerce_uuid(job_id),
            status=status,
            payload=None,
            image_bytes=None,
            content_type=None,
            failure_reason=failure_reason,
            error=error,
        )
        await self.resolve(result)

    async def wait_for_result(
        self,
        job_id: uuid.UUID,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.1,
        is_disconnected: Callable[[], Awaitable[bool]] | None = None,
    ) -> SyncCallbackResult:
        await self._local.register(job_id)

        cached = await self._fetch_result(job_id)
        if cached is not None:
            await self._local.resolve(cached)

        channel = self._channel(job_id)
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)
        pump_task = asyncio.create_task(self._pump_pubsub(pubsub, job_id))

        try:
            return await self._local.wait_for_result(
                job_id,
                timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
                is_disconnected=is_disconnected,
            )
        finally:
            pump_task.cancel()
            try:
                await pump_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            try:
                await pubsub.unsubscribe(channel)
            except Exception:  # pragma: no cover - defensive
                logger.debug("redis_waiter.unsubscribe_failed channel=%s", channel)
            try:
                await pubsub.close()
            except Exception:  # pragma: no cover - defensive
                logger.debug("redis_waiter.close_failed channel=%s", channel)

    async def _pump_pubsub(self, pubsub: Any, job_id: uuid.UUID) -> None:
        channel = self._channel(job_id)
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is None:
                    await asyncio.sleep(0.01)
                    continue
                raw_channel = message.get("channel")
                if isinstance(raw_channel, bytes):
                    raw_channel = raw_channel.decode()
                if raw_channel != channel:
                    continue
                raw_data = message.get("data")
                result = self._deserialize_result(raw_data)
                if result is not None:
                    await self._local.resolve(result)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("redis_waiter.pubsub_error job_id=%s error=%s", job_id, exc)

    async def _store_result(self, result: SyncCallbackResult) -> None:
        try:
            payload = self._serialize_result(result)
            await self._redis.set(self._result_key(result.job_id), payload, ex=self._ttl)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("redis_waiter.store_failed job_id=%s error=%s", result.job_id, exc)

    async def _publish_result(self, result: SyncCallbackResult) -> None:
        try:
            await self._redis.publish(self._channel(result.job_id), self._serialize_result(result))
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("redis_waiter.publish_failed job_id=%s error=%s", result.job_id, exc)

    async def _fetch_result(self, job_id: uuid.UUID) -> SyncCallbackResult | None:
        try:
            raw = await self._redis.get(self._result_key(job_id))
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("redis_waiter.fetch_failed job_id=%s error=%s", job_id, exc)
            return None
        if raw is None:
            return None
        return self._deserialize_result(raw)

    def _serialize_result(self, result: SyncCallbackResult) -> str:
        image_b64 = base64.b64encode(result.image_bytes).decode("utf-8") if result.image_bytes is not None else None
        payload = {
            "job_id": str(result.job_id),
            "status": result.status.value if isinstance(result.status, JobStatus) else str(result.status),
            "payload": result.payload,
            "image_b64": image_b64,
            "content_type": result.content_type,
            "failure_reason": result.failure_reason,
            "error": result.error,
        }
        return json.dumps(payload)

    def _deserialize_result(self, raw: Any) -> SyncCallbackResult | None:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            if isinstance(raw, (int, float)):
                raw = str(raw)
            data = json.loads(raw)
            job_id = uuid.UUID(str(data.get("job_id")))
            status_value = data.get("status") or JobStatus.FAILED.value
            status = JobStatus(status_value) if status_value in JobStatus._value2member_map_ else JobStatus.FAILED
            image_bytes = base64.b64decode(data["image_b64"]) if data.get("image_b64") else None
            return SyncCallbackResult(
                job_id=job_id,
                status=status,
                payload=data.get("payload"),
                image_bytes=image_bytes,
                content_type=data.get("content_type"),
                failure_reason=data.get("failure_reason"),
                error=data.get("error"),
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("redis_waiter.deserialize_failed raw=%s error=%s", raw, exc)
            return None

    def _result_key(self, job_id: uuid.UUID | str) -> str:
        return f"{self._prefix}:result:{job_id}"

    def _channel(self, job_id: uuid.UUID | str) -> str:
        return f"{self._prefix}:channel:{job_id}"

    @staticmethod
    def _coerce_uuid(value: uuid.UUID | str) -> uuid.UUID:
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except Exception:  # pragma: no cover - defensive
            return uuid.uuid5(uuid.NAMESPACE_DNS, str(value))


# Default in-memory alias used by the rest of the codebase
SyncCallbackWaiter = InMemorySyncCallbackWaiter

__all__ = [
    "SyncCallbackResult",
    "BaseSyncCallbackWaiter",
    "SyncCallbackWaiter",
    "InMemorySyncCallbackWaiter",
    "RedisSyncCallbackWaiter",
]
