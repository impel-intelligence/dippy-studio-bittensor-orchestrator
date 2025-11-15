from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Optional

BT_IMPORT_ERROR: Exception | None = None
_BT_IMPORT_LOGGED = False

try:  # pragma: no cover - defensive guard, exercised in container
    import bittensor as bt  # type: ignore
    from bittensor.core.chain_data import decode_account_id  # type: ignore
except Exception as exc:  # noqa: BLE001 - external dependency guard
    bt = None  # type: ignore[assignment]
    decode_account_id = None  # type: ignore[assignment]
    BT_IMPORT_ERROR = exc
else:
    BT_IMPORT_ERROR = None

from orchestrator.domain.miner import Miner
from orchestrator.runners.metagraph import StateResult
from orchestrator.common.model_utils import dump_model

logger = logging.getLogger(__name__)


@dataclass
class MinerRegistry:

    address: str
    port: str

    @classmethod
    def from_compressed_str(cls, payload: str) -> "MinerRegistry":
        tokens = payload.split(":", 1)
        if len(tokens) != 2:
            raise ValueError(f"Invalid miner registry payload: {payload}")
        return cls(address=tokens[0], port=tokens[1])


def _extract_raw_data(data: dict) -> Optional[str]:
    try:
        info = data.get("info", {})
        fields = info.get("fields", ())
        if fields and isinstance(fields[0], tuple) and isinstance(fields[0][0], dict):
            raw_dict = fields[0][0]
            raw_key = next((k for k in raw_dict.keys() if str(k).startswith("Raw")), None)
            if raw_key and raw_dict[raw_key]:
                raw_value = raw_dict[raw_key]
                if isinstance(raw_value, (list, tuple)) and raw_value:
                    numbers = raw_value[0]
                    if isinstance(numbers, (list, tuple)):
                        return "".join(chr(int(x)) for x in numbers)
    except Exception:
        pass
    return None


class SubnetStateClient:

    def __init__(
        self,
        *,
        network: str,
        subtensor: Any | None = None,
    ) -> None:
        self._network = network
        self._subtensor = subtensor

    @staticmethod
    def _format_network_address(registry: MinerRegistry) -> str:
        address = registry.address.strip()
        port = registry.port.strip()

        if ":" in address and not address.startswith("["):
            address = f"[{address}]"

        base = f"https://{address}"
        if port:
            base = f"{base}:{port}"
        return base

    def _extract_alpha_stake(
        self,
        metagraph: Any,
        index: int,
        *,
        hotkey: str | None,
        netuid: int,
        network: str,
    ) -> int:
        alpha_source = getattr(metagraph, "alpha_stake", None)
        if alpha_source is None:
            alpha_source = getattr(metagraph, "stake", None)
            if alpha_source is None:
                logger.warning(
                    "subnet.alpha_stake.source_missing netuid=%s network=%s hotkey=%s index=%s",
                    netuid,
                    network,
                    hotkey,
                    index,
                )
                return 0

        try:
            source_length = len(alpha_source)
        except Exception:
            source_length = None

        if source_length is not None and index >= source_length:
            logger.warning(
                "subnet.alpha_stake.index_out_of_range netuid=%s network=%s hotkey=%s index=%s length=%s",
                netuid,
                network,
                hotkey,
                index,
                source_length,
            )
            return 0

        try:
            value = alpha_source[index]
        except Exception as exc:
            logger.warning(
                "subnet.alpha_stake.access_failed netuid=%s network=%s hotkey=%s index=%s error=%s",
                netuid,
                network,
                hotkey,
                index,
                exc,
            )
            return 0

        try:
            return int(max(0, float(value)))
        except Exception as exc:  # noqa: BLE001 - defensive casting
            logger.warning(
                "subnet.alpha_stake.parse_failed netuid=%s network=%s hotkey=%s index=%s raw_value=%s error=%s",
                netuid,
                network,
                hotkey,
                index,
                value,
                exc,
            )
            return 0

    def _ensure_subtensor(self, network: str | None = None) -> Any | None:
        if network and network != self._network:
            self._network = network
            self._subtensor = None

        if self._subtensor is not None:
            return self._subtensor

        if bt is None:
            global _BT_IMPORT_LOGGED
            if not _BT_IMPORT_LOGGED:
                logger.warning(
                    "subnet.bittensor_unavailable network=%s", self._network, exc_info=BT_IMPORT_ERROR
                )
                _BT_IMPORT_LOGGED = True
            return None

        try:
            self._subtensor = bt.subtensor(network=self._network)
        except Exception as exc:  # noqa: BLE001 - external dependency guard
            logger.warning(
                "subnet.subtensor_init_failed network=%s", self._network, exc_info=exc
            )
            return None

        return self._subtensor

    def fetch_state(self, netuid: int, network: str) -> Optional[StateResult]:
        subtensor = self._ensure_subtensor(network)
        if subtensor is None:
            return None

        try:
            metagraph = subtensor.metagraph(netuid=netuid)
        except Exception as exc:  # noqa: BLE001 - external dependency guard
            logger.warning(
                "subnet.fetch.metagraph_failed netuid=%s network=%s",
                netuid,
                network,
                exc_info=exc,
            )
            return None

        if decode_account_id is None:
            global _BT_IMPORT_LOGGED
            if not _BT_IMPORT_LOGGED:
                logger.warning(
                    "subnet.decode_account_id_unavailable netuid=%s network=%s",
                    netuid,
                    network,
                    exc_info=BT_IMPORT_ERROR,
                )
                _BT_IMPORT_LOGGED = True
            return None

        try:
            raw_commitments = list(
                subtensor.query_map(
                    module="Commitments",
                    name="CommitmentOf",
                    params=[netuid],
                )
            )
        except Exception as exc:  # noqa: BLE001 - external dependency guard
            logger.warning(
                "subnet.fetch.commitment_query_failed netuid=%s network=%s",
                netuid,
                network,
                exc_info=exc,
            )
            return None

        uid_by_hotkey = {
            str(hotkey): int(uid)
            for hotkey, uid in zip(metagraph.hotkeys, metagraph.uids)
        }
        index_by_hotkey = {
            str(hotkey): idx for idx, hotkey in enumerate(metagraph.hotkeys)
        }

        state: dict[str, Miner] = {}

        for key, value in raw_commitments:
            try:
                substrate_key = key[0]
                hotkey = decode_account_id(substrate_key)
            except Exception:  # noqa: BLE001 - decoding guard
                continue

            hotkey_str = str(hotkey)
            if hotkey_str not in uid_by_hotkey or hotkey_str not in index_by_hotkey:
                continue

            commitment_body = getattr(value, "value", None) or {}
            if not isinstance(commitment_body, dict):
                continue

            chain_str = _extract_raw_data(commitment_body)
            if not chain_str:
                continue

            try:
                registry = MinerRegistry.from_compressed_str(chain_str)
            except Exception:  # noqa: BLE001 - commitment parsing guard
                continue

            network_address = self._format_network_address(registry)
            uid = uid_by_hotkey[hotkey_str]
            index = index_by_hotkey[hotkey_str]

            state[hotkey_str] = Miner(
                uid=uid,
                network_address=network_address,
                valid=False,
                alpha_stake=0,
                hotkey=hotkey_str,
            )

        if not state:
            return None

        block: Optional[int] = getattr(metagraph, "block", None)
        if block is not None:
            try:
                block = int(block)
            except Exception:  # noqa: BLE001 - casting guard
                block = None

        return state, block

    def populate_alpha_stake(
        self,
        miners: Mapping[str, Miner],
        *,
        netuid: int,
        network: str,
    ) -> dict[str, Miner]:
        if not miners:
            return {}

        subtensor = self._ensure_subtensor(network)
        if subtensor is None:
            return dict(miners)

        try:
            metagraph = subtensor.metagraph(netuid=netuid)
        except Exception as exc:  # noqa: BLE001 - external dependency guard
            logger.warning(
                "subnet.alpha_stake.metagraph_failed netuid=%s network=%s",
                netuid,
                network,
                exc_info=exc,
            )
            return dict(miners)

        index_by_hotkey = {
            str(hotkey): idx for idx, hotkey in enumerate(metagraph.hotkeys)
        }

        updated: dict[str, Miner] = {str(key): value for key, value in miners.items()}
        for hotkey, miner in updated.items():
            index = index_by_hotkey.get(hotkey)
            if index is None:
                logger.warning(
                    "subnet.alpha_stake.hotkey_missing netuid=%s network=%s hotkey=%s",
                    netuid,
                    network,
                    hotkey,
                )
                continue

            alpha_stake = self._extract_alpha_stake(
                metagraph,
                index,
                hotkey=hotkey,
                netuid=netuid,
                network=network,
            )
            try:
                miner.alpha_stake = alpha_stake
            except Exception:
                payload = dump_model(miner)
                payload["alpha_stake"] = alpha_stake
                updated[hotkey] = Miner(**payload)

        return updated


__all__ = ["SubnetStateClient"]
