import json
import logging
import random
import uuid
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Deque, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field
from psycopg.types.json import Json

from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.common.model_utils import dump_model, validate_model
from orchestrator.services.database_service import DatabaseService


class Miner(BaseModel):
    uid: int
    network_address: str
    valid: bool
    alpha_stake: int
    capacity: Dict[str, object] = Field(default_factory=dict)
    hotkey: Optional[str] = None


class MinerMetagraphClient(ABC):
    @abstractmethod
    def update_state(
        self,
        state: Dict[str, Miner],
        *,
        block: Optional[int] = None,
        fetched_at: Optional[datetime] = None,
    ) -> None:
        pass

    @abstractmethod
    def dump_state(self):
        pass

PLACEHOLDER_MINER = Miner(
    uid=74,
    valid=True,
    network_address="https://tmp-test.dippy-bittensor-subnet.com",
    alpha_stake=100000,
    capacity={},
    hotkey="5EtM9iXMAYRsmt6aoQAoWNDX6yaBnjhmnEQhWKv8HpwkVtML"
)
class LiveMinerMetagraphClient(MinerMetagraphClient):
    def __init__(
        self,
        database_service: DatabaseService,
        epistula_client: Optional[EpistulaClient] = None,
    ) -> None:
        self._instance_id = f"metagraph-{uuid.uuid4().hex}"
        self._database_service = database_service
        self._dsn = self._database_service.dsn
        self._ensure_schema()
        self._max_alpha_limit = 500
        self._score_floor = 0.05
        self._last_update: Optional[datetime] = None
        self._last_block: Optional[int] = None
        self._epistula_client = epistula_client
        self._last_candidates: Deque[Tuple[datetime, Miner]] = deque(maxlen=1000)

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def db_path(self) -> str:
        return self._database_service.safe_dsn

    def _ensure_schema(self) -> None:
        with self._database_service.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS miners (
                    hotkey TEXT PRIMARY KEY,
                    value  JSONB NOT NULL,
                    scores JSONB
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE miners
                ADD COLUMN IF NOT EXISTS scores JSONB
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_miners_valid
                ON miners ((value->>'valid'))
                """
            )

    def validate_state(self, state: Dict[str, Miner]) -> Dict[str, Miner]:
        logger = logging.getLogger(__name__)
        validated_state: Dict[str, Miner] = {}

        for key, value in state.items():
            if not isinstance(value, Miner):
                validated_state[key] = value
                continue

            network_valid = False
            network_address = value.network_address
            if network_address and network_address.strip():
                address_candidate = network_address
                try:
                    parsed = urlparse(address_candidate)
                    if not parsed.scheme:
                        address_candidate = f"https://{address_candidate}/check/{value.hotkey}"
                        parsed = urlparse(address_candidate)

                    if parsed.netloc:
                        req = Request(address_candidate, method="GET")
                        with urlopen(req, timeout=10) as response:
                            if response.status == 200:
                                network_valid = True
                                if validated_state and self._epistula_client:
                                    try:
                                        capacity_url = f"{parsed.scheme}://{parsed.netloc}/capacity"
                                        status_code, response_text = self._epistula_client.get_signed_request_sync(
                                            url=capacity_url,
                                            miner_hotkey=value.hotkey,
                                            timeout=10,
                                        )
                                        if status_code == 200:
                                            value.capacity = json.loads(response_text)
                                    except (URLError, HTTPError, json.JSONDecodeError, Exception) as error:
                                        logger.debug(
                                            "Failed to fetch capacity from %s: %s", capacity_url, error
                                        )
                except (URLError, HTTPError, Exception) as error:
                    logger.debug("Network address %s unreachable: %s", address_candidate, error)

            updated_miner = Miner(
                uid=value.uid,
                network_address=value.network_address,
                valid=network_valid,
                alpha_stake=value.alpha_stake,
                capacity=value.capacity,
                hotkey=value.hotkey,
            )
            validated_state[key] = updated_miner

        return validated_state

    def _miner_to_payload(self, miner: Miner) -> dict[str, object]:
        return dump_model(miner)

    def _payload_to_miner(self, payload: object) -> Miner:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            payload = payload.decode()
        if isinstance(payload, str):
            data = json.loads(payload)
        else:
            data = payload
        return validate_model(Miner, data)

    def _coerce_miner(self, payload: Miner | Mapping[str, Any]) -> Miner:
        if isinstance(payload, Miner):
            return payload
        return validate_model(Miner, dict(payload))

    def _coerce_scores_payload(self, payload: object) -> Optional[Mapping[str, Any]]:
        if payload in (None, {}, ()):  # fast path for empty values
            return None
        if isinstance(payload, Mapping):
            return dict(payload)
        if isinstance(payload, (bytes, bytearray, memoryview)):
            payload = payload.decode()
        if isinstance(payload, str):
            try:
                data = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                return None
            if isinstance(data, Mapping):
                return dict(data)
            return None
        return None

    def _score_multiplier_from_payload(self, payload: Optional[Mapping[str, Any]]) -> float:
        if not payload:
            return 1.0
        if bool(payload.get("is_slashed", False)):
            return 0.0

        def _coerce_float(value: Any) -> Optional[float]:
            try:
                if value is None:
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        candidates = []
        for key in ("ema_score", "scores"):
            coerced = _coerce_float(payload.get(key))
            if coerced is None:
                continue
            candidates.append(max(0.0, min(coerced, 1.0)))

        if not candidates:
            return 1.0

        score_component = max(candidates)
        if score_component <= 0.0:
            return self._score_floor
        return max(self._score_floor, min(score_component, 1.0))

    def _clone_miner(self, miner: Miner) -> Miner:
        if hasattr(miner, "model_copy"):
            return miner.model_copy(deep=True)  # type: ignore[attr-defined]
        if hasattr(miner, "copy"):
            return miner.copy(deep=True)  # type: ignore[attr-defined]
        return self._coerce_miner(miner)

    def _record_candidate(self, miner: Miner) -> None:
        try:
            clone = self._clone_miner(miner)
        except Exception:  # pragma: no cover - defensive guard
            clone = miner
        self._last_candidates.append((datetime.now(timezone.utc), clone))

    def get_last_candidates(self) -> list[Tuple[datetime, Miner]]:
        return list(self._last_candidates)

    def upsert_miner(self, miner: Miner | Mapping[str, Any]) -> Miner:
        model = self._coerce_miner(miner)
        if not model.hotkey:
            raise ValueError("Miner record must include a hotkey")

        with self._database_service.cursor() as cur:
            cur.execute(
                """
                INSERT INTO miners (hotkey, value)
                VALUES (%s, %s)
                ON CONFLICT(hotkey) DO UPDATE
                SET value = EXCLUDED.value
                RETURNING value
                """,
                (
                    model.hotkey,
                    Json(self._miner_to_payload(model)),
                ),
            )
            stored = cur.fetchone()

        self._last_update = datetime.now(timezone.utc)
        return self._payload_to_miner(stored[0]) if stored else model

    def get_miner(self, hotkey: str) -> Optional[Miner]:
        with self._database_service.cursor() as cur:
            cur.execute(
                "SELECT value FROM miners WHERE hotkey = %s",
                (hotkey,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return self._payload_to_miner(row[0])

    def delete_miner(self, hotkey: str) -> bool:
        with self._database_service.cursor() as cur:
            cur.execute(
                "DELETE FROM miners WHERE hotkey = %s",
                (hotkey,),
            )
            deleted = cur.rowcount > 0
        if deleted:
            self._last_update = datetime.now(timezone.utc)
        return deleted

    def update_state(
        self,
        state: Dict[str, Miner],
        *,
        block: Optional[int] = None,
        fetched_at: Optional[datetime] = None,
    ) -> None:
        new_keys = set(state.keys())

        with self._database_service.cursor() as cur:
            cur.execute("SELECT hotkey FROM miners")
            existing_keys = {row[0] for row in cur.fetchall()}
            to_delete = existing_keys - new_keys
            if to_delete:
                cur.executemany(
                    "DELETE FROM miners WHERE hotkey = %s",
                    [(k,) for k in to_delete],
                )

            rows = [
                (
                    hotkey,
                    Json(self._miner_to_payload(miner)),
                )
                for hotkey, miner in state.items()
            ]
            if rows:
                cur.executemany(
                    """
                    INSERT INTO miners (hotkey, value)
                    VALUES (%s, %s)
                    ON CONFLICT(hotkey) DO UPDATE SET value = EXCLUDED.value
                    """,
                    rows,
                )

        self._last_update = fetched_at or datetime.now(timezone.utc)
        self._last_block = block

    def _iter_miners(self, *, valid_only: bool = False):
        query = "SELECT hotkey, value FROM miners"
        if valid_only:
            query += " WHERE (value->>'valid')::boolean IS TRUE"

        with self._database_service.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        logger = logging.getLogger(__name__)
        for hotkey, payload in rows:
            try:
                yield hotkey, self._payload_to_miner(payload)
            except Exception as exc:  # pragma: no cover - defensive skip
                logger.debug(
                    "Skipping miner payload for hotkey %s due to parse error: %s",
                    hotkey,
                    exc,
                )

    def dump_state(self) -> Dict[str, Miner]:
        return {hotkey: miner for hotkey, miner in self._iter_miners()}

    def dump_filtered_state(self) -> Dict[str, Miner]:
        return {hotkey: miner for hotkey, miner in self._iter_miners(valid_only=True)}

    def dump_full_state(self) -> Dict[str, Miner]:
        """Return the complete metagraph state without filtering."""
        return self.dump_state()

    def last_update(self) -> Optional[datetime]:
        return self._last_update

    def last_block(self) -> Optional[int]:
        return self._last_block

    def fetch_miners(self) -> Dict[str, Miner]:
        return {hotkey: miner for hotkey, miner in self._iter_miners(valid_only=True)}

    def fetch_candidate(self) -> Optional[Miner]:
        """Return a randomly selected valid Miner from current state.

        Selection is weighted by the capped stake and adjusted by the miner's most
        recent score/ema_score payload when available. Slashed miners are assigned a
        zero weight, and miners without score data retain their stake-only weighting.

        Only considers miners marked as valid. If no valid miners exist,
        returns None.
        """
        with self._database_service.cursor() as cur:
            cur.execute(
                "SELECT value, scores FROM miners WHERE (value->>'valid')::boolean IS TRUE"
            )
            rows = cur.fetchall()

        logger = logging.getLogger(__name__)
        candidates: list[Tuple[Miner, float]] = []
        for value_payload, score_payload in rows:
            try:
                miner = self._payload_to_miner(value_payload)
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.debug(
                    "Skipping miner payload due to parse error when selecting candidate: %s",
                    exc,
                )
                continue

            stake_component = max(0, min(miner.alpha_stake, self._max_alpha_limit))
            scores_mapping = self._coerce_scores_payload(score_payload)
            score_multiplier = self._score_multiplier_from_payload(scores_mapping)
            weight = stake_component * score_multiplier
            candidates.append((miner, weight))

        if not candidates:
            return None

        weights = [weight for _, weight in candidates]

        if sum(weights) <= 0:
            selected = random.choice([miner for miner, _ in candidates])
        else:
            selected = random.choices(
                [miner for miner, _ in candidates],
                weights=weights,
                k=1,
            )[0]

        self._record_candidate(selected)
        return selected
