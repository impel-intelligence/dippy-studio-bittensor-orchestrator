"""Configuration loading helpers for the orchestrator service."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml


DEFAULT_CONFIG_PATH = Path("orchestrator/config.yaml")
FALLBACK_CONFIG_PATH = Path("orchestrator/config.example.yaml")


@dataclass
class DatabaseConfig:
    url: Optional[str] = None
    min_connections: int = 1
    max_connections: int = 10


@dataclass
class MetagraphConfig:
    table: str = "miners"


@dataclass
class SubnetConfig:
    netuid: int = 11
    network: str = "finney"


@dataclass
class GCSConfig:
    bucket: Optional[str] = None
    prefix: str = "callbacks"
    credentials_path: Optional[Path] = None


@dataclass
class CallbackConfig:
    uploader: str = "noop"
    gcs: GCSConfig = field(default_factory=GCSConfig)
    url: Optional[str] = None

    def effective_uploader(self) -> str:
        if self.uploader.lower() == "gcs" and self.gcs.bucket:
            return "gcs"
        return "noop"

    def resolved_callback_url(self) -> Optional[str]:
        return self.url


@dataclass
class ListenSyncConfig:
    timeout_seconds: float = 10.0
    poll_interval_seconds: float = 1.0
    backend: str = "redis"
    redis_url: Optional[str] = None
    redis_result_ttl_seconds: float = 600.0
    redis_channel_prefix: str = "listen_sync"

    def normalized(self) -> "ListenSyncConfig":
        timeout = _maybe_float(self.timeout_seconds, 10.0) or 10.0
        if timeout < 0.0:
            timeout = 0.0
        poll = _maybe_float(self.poll_interval_seconds, 1.0) or 1.0
        if poll <= 0.0:
            poll = 0.1
        ttl = _maybe_float(self.redis_result_ttl_seconds, 600.0) or 600.0
        if ttl <= 0.0:
            ttl = 60.0
        backend = (self.backend or "redis").strip().lower()
        if backend != "redis":
            raise ValueError(f"Unsupported listen sync backend: {backend}")
        channel_prefix = (self.redis_channel_prefix or "listen_sync").strip() or "listen_sync"
        return ListenSyncConfig(
            timeout_seconds=timeout,
            poll_interval_seconds=poll,
            backend=backend,
            redis_url=self.redis_url,
            redis_result_ttl_seconds=ttl,
            redis_channel_prefix=channel_prefix,
        )


@dataclass
class ListenConfig:
    sync: ListenSyncConfig = field(default_factory=ListenSyncConfig)


@dataclass
class JobRelayConfig:
    enabled: bool = True
    base_url: Optional[str] = "http://localhost:8181"
    auth_token: Optional[str] = None
    timeout_seconds: float = 5.0

    def is_enabled(self) -> bool:
        return self.enabled and bool(self.base_url)


@dataclass
class ScoreConfig:
    ema_alpha: float = 1.0
    ema_half_life_seconds: float = 604_800.0
    failure_penalty_weight: float = 0.2
    lookback_days: float = 7.0

    def normalized(self) -> "ScoreConfig":
        alpha = float(self.ema_alpha)
        if alpha < 0.0:
            alpha = 0.0
        elif alpha > 1.0:
            alpha = 1.0

        half_life = float(self.ema_half_life_seconds)
        if half_life <= 0.0:
            half_life = 604_800.0

        penalty = float(self.failure_penalty_weight)
        if penalty < 0.0:
            penalty = 0.0

        lookback = float(self.lookback_days)
        if lookback < 0.0:
            lookback = 0.0

        return ScoreConfig(
            ema_alpha=alpha,
            ema_half_life_seconds=half_life,
            failure_penalty_weight=penalty,
            lookback_days=lookback,
        )


@dataclass
class OrchestratorConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    metagraph: MetagraphConfig = field(default_factory=MetagraphConfig)
    subnet: SubnetConfig = field(default_factory=SubnetConfig)
    callback: CallbackConfig = field(default_factory=CallbackConfig)
    listen: ListenConfig = field(default_factory=ListenConfig)
    jobrelay: JobRelayConfig = field(default_factory=JobRelayConfig)
    scores: ScoreConfig = field(default_factory=ScoreConfig)
    audit_sample_size: float = 0.1
    metagraph_runner_interval: float = 300.0
    audit_target_domain: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrchestratorConfig":
        database_data = data.get("database", {}) or {}
        database_url_raw = (
            database_data.get("url")
            or database_data.get("dsn")
            or database_data.get("connection")
            or database_data.get("path")
            or database_data.get("db_path")
        )
        min_connections = _maybe_int(database_data.get("min_connections"), 1) or 1
        max_connections = _maybe_int(database_data.get("max_connections"), 10) or 10
        database = DatabaseConfig(
            url=str(database_url_raw).strip() if database_url_raw else None,
            min_connections=max(1, min_connections),
            max_connections=max(1, max_connections),
        )

        metagraph_data = data.get("metagraph", {}) or {}
        metagraph_table = metagraph_data.get("table", "miners")
        metagraph = MetagraphConfig(table=str(metagraph_table).strip() or "miners")

        subnet_data = data.get("subnet", {}) or {}
        netuid_value = _maybe_int(subnet_data.get("netuid"), 11)
        if netuid_value is None:
            netuid_value = 11
        network_value = str(subnet_data.get("network", "finney") or "finney").strip() or "finney"
        subnet = SubnetConfig(netuid=netuid_value, network=network_value)

        callback_data = data.get("callback", {}) or {}
        uploader_value = str(callback_data.get("uploader", "noop")).lower()
        gcs_data = callback_data.get("gcs", {}) or {}
        gcs_credentials = gcs_data.get("credentials_path")
        callback = CallbackConfig(
            uploader=uploader_value,
            gcs=GCSConfig(
                bucket=gcs_data.get("bucket"),
                prefix=gcs_data.get("prefix", "callbacks"),
                credentials_path=Path(gcs_credentials) if gcs_credentials else None,
            ),
            url=callback_data.get("url") or callback_data.get("callback_url"),
        )

        listen_data = data.get("listen", {}) or {}
        sync_data = listen_data.get("sync", {}) or {}
        listen_defaults = ListenSyncConfig()
        sync_timeout = _maybe_float(sync_data.get("timeout_seconds"), listen_defaults.timeout_seconds)
        if sync_timeout is None:
            sync_timeout = listen_defaults.timeout_seconds
        sync_poll = _maybe_float(
            sync_data.get("poll_interval_seconds"),
            listen_defaults.poll_interval_seconds,
        )
        if sync_poll is None:
            sync_poll = listen_defaults.poll_interval_seconds
        backend_value = str(sync_data.get("backend", listen_defaults.backend) or listen_defaults.backend)
        redis_url_value = sync_data.get("redis_url") or listen_defaults.redis_url
        ttl_value = _maybe_float(
            sync_data.get("redis_result_ttl_seconds"),
            listen_defaults.redis_result_ttl_seconds,
        )
        if ttl_value is None:
            ttl_value = listen_defaults.redis_result_ttl_seconds
        prefix_value = sync_data.get("redis_channel_prefix", listen_defaults.redis_channel_prefix)
        listen = ListenConfig(
            sync=ListenSyncConfig(
                timeout_seconds=sync_timeout,
                poll_interval_seconds=sync_poll,
                backend=backend_value,
                redis_url=redis_url_value,
                redis_result_ttl_seconds=ttl_value,
                redis_channel_prefix=prefix_value,
            ).normalized()
        )

        jobrelay_defaults = JobRelayConfig()
        jobrelay_data = data.get("jobrelay", {}) or {}
        enabled_raw = jobrelay_data.get("enabled")
        if enabled_raw is None:
            enabled_value = jobrelay_defaults.enabled
        elif isinstance(enabled_raw, str):
            enabled_value = enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
        else:
            enabled_value = bool(enabled_raw)

        base_url_value = jobrelay_data.get("base_url", jobrelay_defaults.base_url)
        timeout_value = jobrelay_data.get("timeout_seconds", jobrelay_defaults.timeout_seconds)
        jobrelay_timeout = _maybe_float(timeout_value, jobrelay_defaults.timeout_seconds)
        if jobrelay_timeout is None:
            jobrelay_timeout = jobrelay_defaults.timeout_seconds

        jobrelay = JobRelayConfig(
            enabled=enabled_value,
            base_url=base_url_value or jobrelay_defaults.base_url,
            auth_token=jobrelay_data.get("auth_token", jobrelay_defaults.auth_token),
            timeout_seconds=jobrelay_timeout,
        )

        scores_data = data.get("scores", {}) or {}
        ema_alpha_value = _maybe_float(scores_data.get("ema_alpha"), 1.0)
        if ema_alpha_value is None:
            ema_alpha_value = 1.0
        half_life_value = _maybe_float(scores_data.get("ema_half_life_seconds"), 604_800.0)
        if half_life_value is None:
            half_life_value = 604_800.0
        penalty_value = _maybe_float(scores_data.get("failure_penalty_weight"), 0.2)
        if penalty_value is None:
            penalty_value = 0.2
        lookback_value = _maybe_float(scores_data.get("lookback_days"), 7.0)
        if lookback_value is None:
            lookback_value = 7.0
        scores = ScoreConfig(
            ema_alpha=ema_alpha_value,
            ema_half_life_seconds=half_life_value,
            failure_penalty_weight=penalty_value,
            lookback_days=lookback_value,
        ).normalized()

        audit_sample_size = float(data.get("audit_sample_size", 0.1))
        metagraph_runner_interval = float(data.get("metagraph_runner_interval", 300.0))
        audit_target_domain = (data.get("audit_target_domain") or None)

        return cls(
            database=database,
            metagraph=metagraph,
            callback=callback,
            listen=listen,
            jobrelay=jobrelay,
            scores=scores,
            audit_sample_size=audit_sample_size,
            metagraph_runner_interval=metagraph_runner_interval,
            audit_target_domain=audit_target_domain,
            subnet=subnet,
        )

    def apply_env_overrides(self, env: Mapping[str, str]) -> None:
        if "DATABASE_URL" in env:
            value = env["DATABASE_URL"].strip()
            self.database.url = value or self.database.url

        if "DATABASE_MIN_CONNECTIONS" in env:
            self.database.min_connections = max(
                1,
                _maybe_int(env["DATABASE_MIN_CONNECTIONS"], self.database.min_connections)
                or self.database.min_connections,
            )

        if "DATABASE_MAX_CONNECTIONS" in env:
            self.database.max_connections = max(
                1,
                _maybe_int(env["DATABASE_MAX_CONNECTIONS"], self.database.max_connections)
                or self.database.max_connections,
            )

        if "METAGRAPH_TABLE" in env:
            table = env["METAGRAPH_TABLE"].strip()
            if table:
                self.metagraph.table = table

        subnet_netuid_env = env.get("SUBNET_NETUID") or env.get("NETUID")
        if subnet_netuid_env is not None:
            maybe_netuid = _maybe_int(subnet_netuid_env, self.subnet.netuid)
            if maybe_netuid is not None:
                self.subnet.netuid = maybe_netuid

        subnet_network_env = env.get("SUBNET_NETWORK") or env.get("NETWORK")
        if subnet_network_env:
            value = subnet_network_env.strip()
            if value:
                self.subnet.network = value

        if "AUDIT_SAMPLE_SIZE" in env:
            self.audit_sample_size = _maybe_float(env["AUDIT_SAMPLE_SIZE"], self.audit_sample_size)
        if "AUDIT_TARGET_DOMAIN" in env:
            value = env["AUDIT_TARGET_DOMAIN"].strip()
            self.audit_target_domain = value or None
        if "METAGRAPH_RUN_INTERVAL" in env:
            self.metagraph_runner_interval = _maybe_float(env["METAGRAPH_RUN_INTERVAL"], self.metagraph_runner_interval)

        callback_uploader = env.get("CALLBACK_UPLOADER")
        if callback_uploader:
            self.callback.uploader = callback_uploader.lower()

        if "CALLBACK_GCS_BUCKET" in env:
            self.callback.gcs.bucket = env["CALLBACK_GCS_BUCKET"] or None
            if self.callback.gcs.bucket:
                self.callback.uploader = "gcs"
        if "CALLBACK_GCS_PREFIX" in env:
            self.callback.gcs.prefix = env["CALLBACK_GCS_PREFIX"] or self.callback.gcs.prefix
        credentials_override = env.get("CALLBACK_GCS_CREDENTIALS") or env.get("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_override:
            self.callback.gcs.credentials_path = Path(credentials_override)
        if "CALLBACK_URL" in env:
            self.callback.url = env["CALLBACK_URL"].strip() or None

        if "LISTEN_SYNC_TIMEOUT_SECONDS" in env:
            maybe_timeout = _maybe_float(
                env["LISTEN_SYNC_TIMEOUT_SECONDS"],
                self.listen.sync.timeout_seconds,
            )
            if maybe_timeout is not None:
                self.listen.sync.timeout_seconds = max(0.0, maybe_timeout)
        if "LISTEN_SYNC_POLL_INTERVAL_SECONDS" in env:
            maybe_poll = _maybe_float(
                env["LISTEN_SYNC_POLL_INTERVAL_SECONDS"],
                self.listen.sync.poll_interval_seconds,
            )
            if maybe_poll is not None and maybe_poll > 0.0:
                self.listen.sync.poll_interval_seconds = maybe_poll
        if "LISTEN_SYNC_BACKEND" in env:
            backend_override = env["LISTEN_SYNC_BACKEND"].strip()
            if backend_override:
                backend_value = backend_override.lower()
                if backend_value != "redis":
                    raise ValueError(f"Unsupported listen sync backend override: {backend_override}")
                self.listen.sync.backend = backend_value
        if "LISTEN_SYNC_REDIS_URL" in env:
            url_override = env["LISTEN_SYNC_REDIS_URL"].strip()
            self.listen.sync.redis_url = url_override or None
        if "LISTEN_SYNC_REDIS_TTL_SECONDS" in env:
            ttl_override = _maybe_float(env["LISTEN_SYNC_REDIS_TTL_SECONDS"], self.listen.sync.redis_result_ttl_seconds)
            if ttl_override is not None and ttl_override > 0.0:
                self.listen.sync.redis_result_ttl_seconds = ttl_override
        if "LISTEN_SYNC_REDIS_CHANNEL_PREFIX" in env:
            prefix_override = env["LISTEN_SYNC_REDIS_CHANNEL_PREFIX"].strip()
            if prefix_override:
                self.listen.sync.redis_channel_prefix = prefix_override


        if "EMA_ALPHA" in env:
            alpha = _maybe_float(env["EMA_ALPHA"], self.scores.ema_alpha)
            if alpha is not None:
                self.scores.ema_alpha = alpha
        if "EMA_HALF_LIFE_SECONDS" in env:
            half_life = _maybe_float(env["EMA_HALF_LIFE_SECONDS"], self.scores.ema_half_life_seconds)
            if half_life is not None:
                self.scores.ema_half_life_seconds = half_life
        if "FAILURE_PENALTY_WEIGHT" in env:
            penalty = _maybe_float(env["FAILURE_PENALTY_WEIGHT"], self.scores.failure_penalty_weight)
            if penalty is not None:
                self.scores.failure_penalty_weight = penalty
        lookback_override_env = env.get("SCORES_LOOKBACK_DAYS") or env.get("LOOKBACK_DAYS")
        if lookback_override_env is not None:
            lookback_override = _maybe_float(lookback_override_env, self.scores.lookback_days)
            if lookback_override is not None:
                self.scores.lookback_days = lookback_override

        self.scores = self.scores.normalized()
        self.listen.sync = self.listen.sync.normalized()


def load_config(path: str | Path | None = None, *, env: Mapping[str, str] | None = None) -> OrchestratorConfig:
    env = env or os.environ

    candidate_paths: list[Path] = []
    if path:
        candidate_paths.append(Path(path))
    elif env.get("ORCHESTRATOR_CONFIG_PATH"):
        candidate_paths.append(Path(env["ORCHESTRATOR_CONFIG_PATH"]))

    candidate_paths.extend([DEFAULT_CONFIG_PATH, FALLBACK_CONFIG_PATH])

    config_data: dict[str, Any] = {}
    for candidate in candidate_paths:
        if candidate and candidate.exists():
            with candidate.open("r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
            config_data = loaded if isinstance(loaded, dict) else {}
            break

    config = OrchestratorConfig.from_dict(config_data)
    config.apply_env_overrides(env)
    return config

def _maybe_float(value: Any, default: Optional[float]) -> Optional[float]:
    if value in {None, "", "None"}:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _maybe_int(value: Any, default: Optional[int]) -> Optional[int]:
    if value in {None, "", "None"}:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "OrchestratorConfig",
    "DatabaseConfig",
    "MetagraphConfig",
    "SubnetConfig",
    "CallbackConfig",
    "GCSConfig",
    "ListenConfig",
    "ListenSyncConfig",
    "JobRelayConfig",
    "ScoreConfig",
    "load_config",
]
