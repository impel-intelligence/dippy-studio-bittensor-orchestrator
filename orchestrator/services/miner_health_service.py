from __future__ import annotations

import logging
from typing import Dict

from orchestrator.clients.miner_health_client import MinerHealthClient
from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.domain.miner import Miner
from orchestrator.repositories import MinerRepository


class MinerHealthService:
    """Validate miner state snapshots and update capacity info when available."""

    def __init__(
        self,
        repository: MinerRepository,
        *,
        epistula_client: EpistulaClient | None = None,
        health_client: MinerHealthClient | None = None,
    ) -> None:
        self._repository = repository
        self._health_client = health_client or MinerHealthClient(epistula_client=epistula_client)
        self._logger = logging.getLogger(__name__)

    def validate_state(self, state: Dict[str, Miner]) -> Dict[str, Miner]:
        logger = self._logger
        validated_state: Dict[str, Miner] = {}

        persisted_state: Dict[str, Miner] = {}
        try:
            persisted_state = self._repository.dump_state()
        except Exception as exc:  # pragma: no cover - best effort to preserve validity
            logger.debug("miner_health.persisted_state_failed error=%s", exc)

        for key, value in state.items():
            if not isinstance(value, Miner):
                validated_state[key] = value
                continue

            existing = persisted_state.get(key)
            existing_failed_audits = getattr(existing, "failed_audits", 0) if existing else 0
            incoming_failed_audits = getattr(value, "failed_audits", 0)
            failed_audits = max(existing_failed_audits, incoming_failed_audits)

            network_address = value.network_address
            if not network_address or not network_address.strip():
                resolved_valid = existing.valid if isinstance(existing, Miner) else False
                if failed_audits > 0:
                    resolved_valid = False
                validated_state[key] = Miner(
                    uid=value.uid,
                    network_address=value.network_address,
                    valid=resolved_valid,
                    alpha_stake=value.alpha_stake,
                    capacity=value.capacity,
                    hotkey=value.hotkey,
                    failed_audits=failed_audits,
                )
                continue

            network_valid = self._health_client.check_network_health(network_address, value.hotkey or "")
            capacity_parse_error = False
            if network_valid:
                capacity, capacity_parse_error = self._health_client.fetch_capacity(
                    network_address,
                    value.hotkey or "",
                )
                if capacity is not None:
                    value.capacity = capacity
                    try:
                        capacity_keys = ",".join(sorted(capacity.keys())) if isinstance(capacity, dict) else "<non-mapping>"
                    except Exception:
                        capacity_keys = "<unknown>"
                    logger.info(
                        "miner_health.capacity_fetch_ok url=%s hotkey=%s keys=%s",
                        network_address,
                        value.hotkey or "",
                        capacity_keys,
                    )
                elif capacity_parse_error:
                    value.capacity = {}

            if failed_audits > 0:
                resolved_valid = False
            elif network_valid:
                resolved_valid = True
            elif isinstance(existing, Miner):
                resolved_valid = existing.valid
            else:
                resolved_valid = False
            if capacity_parse_error:
                resolved_valid = False

            validated_state[key] = Miner(
                uid=value.uid,
                network_address=value.network_address,
                valid=resolved_valid,
                alpha_stake=value.alpha_stake,
                capacity=value.capacity,
                hotkey=value.hotkey,
                failed_audits=failed_audits,
            )

        return validated_state

__all__ = ["MinerHealthService"]
