import json
import logging
import random
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field

from orchestrator.common.epistula_client import EpistulaClient
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


class LiveMinerMetagraphClient(MinerMetagraphClient):
    def __init__(self, database_service: DatabaseService, epistula_client: Optional[EpistulaClient] = None) -> None:
        self._instance_id = f"metagraph-{uuid.uuid4().hex}"
        self._database_service = database_service
        self._conn = self._database_service.get_connection()
        self._db_path = str(self._database_service.path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS miners (
                hotkey TEXT PRIMARY KEY,
                value  TEXT NOT NULL CHECK (json_valid(value))
            ) STRICT
            """
        )
        self._conn.commit()
        self._max_alpha_limit = 6000
        self._last_update: Optional[datetime] = None
        self._last_block: Optional[int] = None
        self._epistula_client = epistula_client

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def db_path(self) -> str:
        return self._db_path

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


    def _miner_to_json(self, miner: Miner) -> str:
        if hasattr(miner, "model_dump_json"):
            return miner.model_dump_json()
        if hasattr(miner, "json"):
            return miner.json()
        return json.dumps(getattr(miner, "dict", lambda: miner)())

    def _json_to_miner(self, payload: str) -> Miner:
        if hasattr(Miner, "model_validate_json"):
            return Miner.model_validate_json(payload)  # type: ignore[attr-defined]
        if hasattr(Miner, "parse_raw"):
            return Miner.parse_raw(payload)  # type: ignore[attr-defined]
        return Miner(**json.loads(payload))

    def update_state(
        self,
        state: Dict[str, Miner],
        *,
        block: Optional[int] = None,
        fetched_at: Optional[datetime] = None,
    ) -> None:
        cur = self._conn.cursor()
        new_keys = set(state.keys())
        cur.execute("SELECT hotkey FROM miners")
        existing_keys = {row[0] for row in cur.fetchall()}
        to_delete = existing_keys - new_keys
        if to_delete:
            cur.executemany("DELETE FROM miners WHERE hotkey = ?", [(k,) for k in to_delete])
        rows = [
            (
                hotkey,
                self._miner_to_json(miner),
            )
            for hotkey, miner in state.items()
        ]
        if rows:
            cur.executemany(
                """
                INSERT INTO miners (hotkey, value)
                VALUES (?, ?)
                ON CONFLICT(hotkey) DO UPDATE SET value=excluded.value
                """,
                rows,
            )
        self._conn.commit()
        self._last_update = fetched_at or datetime.now(timezone.utc)
        self._last_block = block

    def _iter_miners(self, *, valid_only: bool = False):
        cur = self._conn.cursor()
        if valid_only:
            cur.execute(
                """
                SELECT hotkey, value
                FROM miners
                WHERE json_extract(value, '$.valid') IN (1, 'true')
                """
            )
        else:
            cur.execute("SELECT hotkey, value FROM miners")

        for hotkey, payload in cur:
            yield hotkey, self._json_to_miner(payload)

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
        """Return a randomly selected valid Miner from current state, weighted by stake
        TODO: Add additional consideration based on miner score once we refactor miner state to include both onchain + cache data 

        Only considers miners marked as valid. If no valid miners exist,
        returns None.
        """
        candidates = [miner for _, miner in self._iter_miners(valid_only=True)]
        if not candidates:
            return None

        weights = [
            max(0, min(miner.alpha_stake, self._max_alpha_limit))
            for miner in candidates
        ]

        if sum(weights) <= 0:
            return random.choice(candidates)

        return random.choices(candidates, weights=weights, k=1)[0]
