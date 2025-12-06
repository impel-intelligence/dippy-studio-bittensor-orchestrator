from __future__ import annotations

import logging
import os
import random
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import Response
from pydantic import BaseModel, Field

_OTEL_CONFIGURED = False
LOGGER = logging.getLogger("forwarder.app")
LISTEN_AUTH_HEADER = "X-Service-Auth-Secret"


def _configure_opentelemetry(app: FastAPI) -> None:
    """Initialize OpenTelemetry tracing/metrics/logging with OTLP exporters.

    Configures trace-log correlation by:
    1. Setting up OTLP exporters for traces, metrics, and logs
    2. Using LoggingInstrumentor to inject trace context into log records
    3. Attaching LoggingHandler to export logs with trace_id/span_id attributes
    """
    global _OTEL_CONFIGURED
    if _OTEL_CONFIGURED:
        return
    if os.getenv("OTEL_SDK_DISABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        LOGGER.info("OpenTelemetry disabled via OTEL_SDK_DISABLED")
        return
    try:
        from opentelemetry import metrics, trace
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        from opentelemetry.instrumentation.logging import LoggingInstrumentor
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:  # pragma: no cover
        LOGGER.warning("OpenTelemetry not configured (missing packages): %s", exc)
        return

    base_otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318").rstrip("/")
    traces_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or f"{base_otlp_endpoint}/v1/traces"
    metrics_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") or f"{base_otlp_endpoint}/v1/metrics"
    logs_endpoint = os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") or f"{base_otlp_endpoint}/v1/logs"

    # Parse OTEL_RESOURCE_ATTRIBUTES for additional resource attributes
    resource_attrs = {"service.name": os.getenv("OTEL_SERVICE_NAME", "forwarder")}
    otel_resource_attrs = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "")
    if otel_resource_attrs:
        for attr in otel_resource_attrs.split(","):
            if "=" in attr:
                key, value = attr.split("=", 1)
                resource_attrs[key.strip()] = value.strip()

    resource = Resource.create(resource_attrs)

    # 1. Configure Tracing
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_endpoint)))
    trace.set_tracer_provider(tracer_provider)

    # 2. Configure Metrics
    metric_exporter = OTLPMetricExporter(endpoint=metrics_endpoint)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(metric_exporter)],
    )
    metrics.set_meter_provider(meter_provider)

    # 3. Configure Logging with OTLP export
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=logs_endpoint)),
    )
    set_logger_provider(logger_provider)

    # 4. CRITICAL: Instrument logging BEFORE creating handlers
    LoggingInstrumentor().instrument(set_logging_format=True)

    # 5. Create OTEL handler that exports logs with trace context
    otel_logging_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

    # 6. Create console handler with trace context format
    console_format = (
        "%(asctime)s %(levelname)s [%(name)s] [trace_id=%(otelTraceID)s span_id=%(otelSpanID)s] - %(message)s"
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(console_format))

    # 7. Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(otel_logging_handler)

    # 8. Configure specific loggers
    for logger_name in ("forwarder", "forwarder.app", "uvicorn", "uvicorn.error", "uvicorn.access"):
        lgr = logging.getLogger(logger_name)
        lgr.handlers.clear()
        lgr.addHandler(console_handler)
        lgr.addHandler(otel_logging_handler)
        lgr.propagate = False
        lgr.setLevel(logging.INFO)

    # 9. Instrument FastAPI and HTTPX
    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )
    HTTPXClientInstrumentor().instrument()

    _OTEL_CONFIGURED = True
    LOGGER.info(
        "OpenTelemetry configured for forwarder (endpoint_base=%s, resource=%s)",
        base_otlp_endpoint,
        resource_attrs,
    )


DEFAULT_LISTEN_SECRET = "orchestrator-listen-secret"
DEFAULT_JOB_TYPE = "base-h100_pcie"
DEFAULT_ORCHESTRATOR_URL = "http://orchestrator:42169"


class Settings(BaseModel):
    orchestrator_base_url: str = Field(default=DEFAULT_ORCHESTRATOR_URL)
    listen_auth_secret: str = Field(default=DEFAULT_LISTEN_SECRET, min_length=1, max_length=255)
    job_type: str = Field(default=DEFAULT_JOB_TYPE, min_length=1, max_length=255)
    request_timeout_seconds: float = Field(default=10.0, gt=0)

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            orchestrator_base_url=_string_env(
                "FORWARDER_ORCHESTRATOR_BASE_URL",
                fallback=os.getenv("ORCHESTRATOR_BASE_URL", DEFAULT_ORCHESTRATOR_URL),
            ),
            listen_auth_secret=_string_env(
                "FORWARDER_LISTEN_AUTH_SECRET",
                fallback=os.getenv("LISTEN_AUTH_SECRET", DEFAULT_LISTEN_SECRET),
            ),
            job_type=_string_env("FORWARDER_JOB_TYPE", fallback=DEFAULT_JOB_TYPE),
            request_timeout_seconds=_float_env("FORWARDER_TIMEOUT_SECONDS", fallback=10.0),
        )


class ForwardRequest(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=255)
    webhook_url: str = Field(..., min_length=1, max_length=255)
    route_to_auditor: bool | None = None


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Forwarder", version="0.1.0")
    _configure_opentelemetry(app)

    app.state.settings = settings
    app.state.http_client = None

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.http_client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        client = getattr(app.state, "http_client", None)
        if client:
            await client.aclose()

    @app.post("/listen/remote", status_code=status.HTTP_202_ACCEPTED)
    async def forward_job(request: ForwardRequest) -> Response:
        route_to_auditor = request.route_to_auditor
        if route_to_auditor is None:
            # Randomly route ~5% of requests to the audit miner.
            route_to_auditor = random.random() < 0.05

        payload = {
            "job_type": settings.job_type,
            "payload": {"prompt": request.prompt},
            "webhook_url": request.webhook_url,
            "route_to_auditor": route_to_auditor,
        }
        target_url = f"{settings.orchestrator_base_url.rstrip('/')}/listen/remote"
        headers = {LISTEN_AUTH_HEADER: settings.listen_auth_secret}

        client: httpx.AsyncClient | None = app.state.http_client
        created_client = False
        if client is None:
            # Fallback for cases where startup has not run yet.
            client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)
            created_client = True

        try:
            resp = await client.post(target_url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to reach orchestrator: {exc}",
            ) from exc
        finally:
            if created_client:
                await client.aclose()

        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=resp.headers.get("content-type"),
        )

    return app


def _string_env(key: str, fallback: str) -> str:
    value = os.getenv(key)
    if value is None:
        return fallback
    return value


def _float_env(key: str, fallback: float) -> float:
    raw_value = os.getenv(key)
    if raw_value is None:
        return fallback
    try:
        return float(raw_value)
    except ValueError:
        return fallback
