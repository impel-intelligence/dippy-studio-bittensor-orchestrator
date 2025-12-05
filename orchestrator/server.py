from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from orchestrator.clients.database import PostgresClient
from orchestrator.clients.jobrelay_client import JobRelayHttpClient, JobRelaySettings
from orchestrator.clients.storage import BaseUploader, GCSUploader
from orchestrator.clients.subnet_state_client import SubnetStateClient
from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.common.server_context import ServerContext
from orchestrator.common.job_store import JobType
from orchestrator.common.stubbing import resolve_audit_miner
from orchestrator.config import OrchestratorConfig, load_config
from orchestrator.dependencies import set_dependencies
from orchestrator.routes import create_internal_router, create_public_router
from orchestrator.services.audit_service import AuditService
from orchestrator.services.callback_service import CallbackService
from orchestrator.services.job_service import JobService
from orchestrator.services.miner_health_service import MinerHealthService
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.services.miner_selection_service import MinerSelectionService
from orchestrator.repositories import AuditFailureRepository, MinerRepository
from orchestrator.services.score_service import ScoreService
from orchestrator.domain.miner import Miner
from orchestrator.services.sync_waiter import RedisSyncCallbackWaiter, SyncCallbackWaiter
from orchestrator.services.webhook_dispatcher import WebhookDispatcher

_OTEL_CONFIGURED = False
logger = logging.getLogger(__name__)


def _configure_opentelemetry(app: FastAPI) -> None:
    """Initialize OpenTelemetry tracing/metrics with OTLP exporters."""
    global _OTEL_CONFIGURED
    if _OTEL_CONFIGURED:
        return
    if os.getenv("OTEL_SDK_DISABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        logger.info("OpenTelemetry disabled via OTEL_SDK_DISABLED")
        return
    try:
        from opentelemetry import metrics, trace
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.logging import LoggingInstrumentor
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler, set_logger_provider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:  # pragma: no cover - defensive logging only
        logger.warning("OpenTelemetry not configured (missing packages): %s", exc)
        return

    base_otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318").rstrip("/")
    traces_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or f"{base_otlp_endpoint}/v1/traces"
    metrics_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") or f"{base_otlp_endpoint}/v1/metrics"
    logs_endpoint = os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") or f"{base_otlp_endpoint}/v1/logs"

    resource = Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "orchestrator")})

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_endpoint)))
    trace.set_tracer_provider(tracer_provider)

    metric_exporter = OTLPMetricExporter(endpoint=metrics_endpoint)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(metric_exporter)],
    )
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=logs_endpoint)),
    )
    set_logger_provider(logger_provider)

    LoggingInstrumentor().instrument(set_logging_format=True)
    otel_logging_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if not any(isinstance(handler, LoggingHandler) for handler in root_logger.handlers):
        root_logger.addHandler(otel_logging_handler)

    orch_logger = logging.getLogger("orchestrator")
    if not any(isinstance(handler, LoggingHandler) for handler in orch_logger.handlers):
        orch_logger.addHandler(otel_logging_handler)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lgr = logging.getLogger(logger_name)
        if not any(isinstance(handler, LoggingHandler) for handler in lgr.handlers):
            lgr.addHandler(otel_logging_handler)

    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )
    RequestsInstrumentor().instrument()

    _OTEL_CONFIGURED = True
    logger.info("OpenTelemetry configured for orchestrator (endpoint_base=%s)", base_otlp_endpoint)

__all__ = ["orchestrator"]

DEFAULT_LOCAL_STUB_MINER_URL = "http://stub-miner:8765"
DEFAULT_LOCAL_STUB_HOTKEY = "5EtM9iXMAYRsmt6aoQAoWNDX6yaBnjhmnEQhWKv8HpwkVtML"


class Orchestrator:  # noqa: D101 – thin wrapper around FastAPI app
    def __init__(
        self,
        *,
        config: OrchestratorConfig | None = None,
        config_path: str | Path | None = None,
        audit_sample_size: float | None = None,
        metagraph_runner_interval: float | None = None,
        database_url: str | None = None,
    ) -> None:  # noqa: D401 – simple init
        self.config = config or load_config(config_path)

        if audit_sample_size is not None:
            self.config.audit_sample_size = audit_sample_size
        if metagraph_runner_interval is not None:
            self.config.metagraph_runner_interval = metagraph_runner_interval
        if database_url is not None:
            self.config.database.url = database_url

        self.miner_registry = None

        resolved_db_url = self.config.database.url or os.getenv("DATABASE_URL")
        if not resolved_db_url:
            resolved_db_url = "postgresql://orchestrator:orchestrator@postgres:5432/orchestrator"

        min_conn = max(1, self.config.database.min_connections)
        max_conn = max(min_conn, self.config.database.max_connections)

        self.config.database.url = resolved_db_url
        self.database_service = PostgresClient(
            dsn=resolved_db_url,
            min_connections=min_conn,
            max_connections=max_conn,
        )

        self.epistula_client = EpistulaClient()
        self.miner_repository = MinerRepository(self.database_service)
        self.audit_failure_repository = AuditFailureRepository(self.database_service)
        self.miner_health_service = MinerHealthService(
            repository=self.miner_repository,
            epistula_client=self.epistula_client,
        )
        self.miner_selection_service = MinerSelectionService(self.miner_repository)
        self.miner_metagraph_service = MinerMetagraphService(
            database_service=self.database_service,
            epistula_client=self.epistula_client,
            repository=self.miner_repository,
            health_service=self.miner_health_service,
            selection_service=self.miner_selection_service,
        )
        self.netuid = self.config.subnet.netuid
        self.network = self.config.subnet.network
        self.subnet_state_service = SubnetStateClient(network=self.network)
        self.audit_miner = resolve_audit_miner(self.config.audit_miner_network_address)

        self.server_context = ServerContext.default(service_name="orchestrator")
        self._maybe_seed_local_stub_miner()

        callback_cfg = self.config.callback
        uploader: BaseUploader
        if callback_cfg.effective_uploader() == "gcs":
            try:
                uploader = GCSUploader(
                    bucket=callback_cfg.gcs.bucket or "",
                    prefix=callback_cfg.gcs.prefix,
                    credentials_path=callback_cfg.gcs.credentials_path,
                )
            except Exception as exc:  # noqa: BLE001
                self.server_context.logger.warning(
                    "callback.gcs_uploader_init_failed",
                    bucket=callback_cfg.gcs.bucket,
                    error=str(exc),
                )
                uploader = BaseUploader()
        else:
            uploader = BaseUploader()

        self.callback_uploader = uploader
        uploader_type = type(self.callback_uploader).__name__
        uploader_bucket = getattr(self.callback_uploader, "bucket", None)
        uploader_prefix = getattr(self.callback_uploader, "prefix", None)
        uploader_creds = getattr(self.callback_uploader, "credentials_path", None)
        self.server_context.logger.info(
            "callback.uploader_selected",
            uploader_type=uploader_type,
            bucket=uploader_bucket,
            prefix=uploader_prefix,
            credentials=str(uploader_creds) if uploader_creds is not None else None,
        )
        self.sync_callback_waiter = self._build_sync_waiter()
        self.webhook_dispatcher = WebhookDispatcher()
        self.callback_service = CallbackService(
            uploader=self.callback_uploader,
            sync_waiter=self.sync_callback_waiter,
            webhook_dispatcher=self.webhook_dispatcher,
        )

        jobrelay_cfg = self.config.jobrelay
        if not jobrelay_cfg.is_enabled():
            raise RuntimeError("Job relay integration must be enabled for the orchestrator")

        if not jobrelay_cfg.base_url:
            raise RuntimeError("Job relay base URL must be configured")

        try:
            jobrelay_settings = JobRelaySettings(
                base_url=jobrelay_cfg.base_url,
                auth_token=jobrelay_cfg.auth_token,
                timeout_seconds=jobrelay_cfg.timeout_seconds,
            )
            self.job_relay_client = JobRelayHttpClient(jobrelay_settings)
        except Exception as exc:  # noqa: BLE001
            self.server_context.logger.error(
                "jobrelay.client_init_failed",
                url=jobrelay_cfg.base_url,
                error=str(exc),
            )
            raise

        self.job_service = JobService(job_relay=self.job_relay_client)
        self.score_service = ScoreService(
            database_service=self.database_service,
            job_service=self.job_service,
            subnet_state_service=self.subnet_state_service,
            netuid=self.netuid,
            network=self.network,
            miner_metagraph_service=self.miner_metagraph_service,
            ema_alpha=self.config.scores.ema_alpha,
            ema_half_life_seconds=self.config.scores.ema_half_life_seconds,
            failure_penalty_weight=self.config.scores.failure_penalty_weight,
            lookback_days=self.config.scores.lookback_days,
        )
        self.server_context.score_service = self.score_service

        self.audit_service = AuditService(
            job_service=self.job_service,
            miner_metagraph_service=self.miner_metagraph_service,
            audit_sample_size=self.config.audit_sample_size,
            logger=self.server_context.logger,
        )

        self.app = FastAPI(title="Orchestrator Service", version="0.0.1")
        self._setup_dependencies_and_routes()

    def _build_sync_waiter(self) -> SyncCallbackWaiter:
        """Select sync waiter backend based on configuration."""
        sync_cfg = self.config.listen.sync
        backend = (sync_cfg.backend or "redis").strip().lower()
        if backend != "redis":
            raise RuntimeError(f"Unsupported sync waiter backend: {backend}")

        redis_url = sync_cfg.redis_url or os.getenv("LISTEN_SYNC_REDIS_URL")
        if not redis_url:
            raise RuntimeError("Redis URL must be configured for sync callback waiter")

        try:
            import redis.asyncio as redis_async  # type: ignore[import-not-found]
        except Exception as exc:  # noqa: BLE001
            self.server_context.logger.error(
                "sync_waiter.redis_dependency_missing",
                backend="redis",
                error=str(exc),
            )
            raise RuntimeError("Redis support is required for sync callback waiter") from exc

        try:
            redis_client = redis_async.from_url(redis_url)
            self.server_context.logger.info(
                "sync_waiter.backend_selected",
                backend="redis",
                redis_url=redis_url,
                prefix=sync_cfg.redis_channel_prefix,
                ttl_seconds=sync_cfg.redis_result_ttl_seconds,
            )
            # Persist the resolved URL for downstream logging and verification.
            self.config.listen.sync.redis_url = redis_url
            return RedisSyncCallbackWaiter(
                redis_client,
                channel_prefix=sync_cfg.redis_channel_prefix,
                result_ttl_seconds=sync_cfg.redis_result_ttl_seconds,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self.server_context.logger.error(
                "sync_waiter.redis_init_failed",
                backend="redis",
                redis_url=redis_url,
                error=str(exc),
            )
            raise RuntimeError("Failed to initialize Redis sync callback waiter") from exc

    @staticmethod
    def _coerce_int(candidate: str | None, default: int) -> int:
        try:
            if candidate is None:
                return default
            return int(candidate)
        except (TypeError, ValueError):
            return default

    def _maybe_seed_local_stub_miner(self) -> None:
        enabled = os.getenv("ENABLE_LOCAL_STUB_MINER", "false").lower() in {"1", "true", "yes", "on"}
        if not enabled:
            return

        base_url = os.getenv("LOCAL_STUB_MINER_URL", DEFAULT_LOCAL_STUB_MINER_URL).strip().rstrip("/")
        hotkey = os.getenv("LOCAL_STUB_MINER_HOTKEY", DEFAULT_LOCAL_STUB_HOTKEY).strip()
        uid = self._coerce_int(os.getenv("LOCAL_STUB_MINER_UID"), 4242)
        alpha_stake = self._coerce_int(os.getenv("LOCAL_STUB_MINER_ALPHA"), 100_000)

        capacity_override = os.getenv("LOCAL_STUB_MINER_CAPACITY")
        if capacity_override:
            capabilities = {
                value.strip(): True
                for value in capacity_override.split(",")
                if value.strip()
            }
        else:
            capabilities = {job_type.value: True for job_type in JobType}

        miner = Miner(
            uid=uid,
            network_address=base_url,
            valid=True,
            alpha_stake=alpha_stake,
            capacity=capabilities,
            hotkey=hotkey,
            failed_audits=0,
            failure_count=0,
        )

        try:
            self.miner_repository.upsert_miner(miner)
            self.server_context.logger.info(
                "dev.stub_miner_seeded",
                url=base_url,
                hotkey=hotkey,
                capacity=list(capabilities.keys()),
            )
        except Exception as exc:  # noqa: BLE001 - defensive guard for dev helper
            self.server_context.logger.error(
                "dev.stub_miner_seed_failed",
                url=base_url,
                hotkey=hotkey,
                error=str(exc),
            )

    def _setup_dependencies_and_routes(self) -> None:  # noqa: D401 – helper method
        set_dependencies(
            miner_metagraph_service=self.miner_metagraph_service,
            database_service=self.database_service,
            server_context=self.server_context,
            callback_service=self.callback_service,
            audit_failure_repository=self.audit_failure_repository,
            job_service=self.job_service,
            job_relay_client=self.job_relay_client,
            config=self.config,
            subnet_state_service=self.subnet_state_service,
            score_service=self.score_service,
            epistula_client=self.epistula_client,
            sync_callback_waiter=self.sync_callback_waiter,
            webhook_dispatcher=self.webhook_dispatcher,
        )
        self.app.include_router(create_internal_router())
        self.app.include_router(create_public_router())

        async def _verify_sync_waiter_connection() -> None:
            self.server_context.logger.info(
                "sync_waiter.connection_check.start",
                redis_url=self.config.listen.sync.redis_url,
                prefix=self.config.listen.sync.redis_channel_prefix,
            )
            if not isinstance(self.sync_callback_waiter, RedisSyncCallbackWaiter):
                raise RuntimeError("Redis sync callback waiter not configured")
            try:
                await self.sync_callback_waiter.ping()
            except Exception as exc:  # noqa: BLE001
                self.server_context.logger.error(
                    "sync_waiter.connection_check_failed",
                    redis_url=self.config.listen.sync.redis_url,
                    prefix=self.config.listen.sync.redis_channel_prefix,
                    error=str(exc),
                )
                raise RuntimeError("Redis connectivity check failed") from exc
            self.server_context.logger.info(
                "sync_waiter.connection_check.ok",
                redis_url=self.config.listen.sync.redis_url,
                prefix=self.config.listen.sync.redis_channel_prefix,
            )

        async def _verify_jobrelay_connection() -> None:
            self.server_context.logger.info(
                "jobrelay.connection_check.start",
                url=self.config.jobrelay.base_url,
                auth_token=self.config.jobrelay.auth_token,
            )
            try:
                await self.job_relay_client.verify_connection()
            except Exception as exc:  # noqa: BLE001
                self.server_context.logger.error(
                    "jobrelay.connection_check_failed",
                    url=self.config.jobrelay.base_url,
                    error=str(exc),
                )
                raise RuntimeError("Job relay connectivity check failed") from exc
            self.server_context.logger.info(
                "jobrelay.connection_check.ok",
                url=self.config.jobrelay.base_url,
            )

        self.app.add_event_handler("startup", _verify_sync_waiter_connection)
        self.app.add_event_handler("startup", _verify_jobrelay_connection)


def create_app() -> FastAPI:
    orch = Orchestrator()
    _configure_opentelemetry(orch.app)
    return orch.app


if __name__ == "__main__":
    default_port = int(os.getenv("ORCHESTRATOR_PORT", "42169"))
    parser = argparse.ArgumentParser(description="Orchestrator Service")
    parser.add_argument(
        "--live-reload",
        action="store_true",
        help="Enable hot reload for development (watches for file changes)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=default_port,
        help="Port to bind the server (default comes from ORCHESTRATOR_PORT or 42169)",
    )
    
    args = parser.parse_args()
    
    uvicorn.run(
        "orchestrator.server:create_app",
        host="0.0.0.0",
        port=args.port,
        reload=args.live_reload,
        factory=True,
    )
