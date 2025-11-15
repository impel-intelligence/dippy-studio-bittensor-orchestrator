from __future__ import annotations

import logging
import random
from collections import deque
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Tuple

from orchestrator.domain.miner import Miner
from orchestrator.repositories import MinerRepository


class MinerSelectionService:
    """Select candidate miners using stake and score weighting."""

    def __init__(
        self,
        repository: MinerRepository,
        *,
        max_alpha_limit: int = 500,
        score_floor: float = 0.05,
        candidate_history: int = 1000,
    ) -> None:
        self._repository = repository
        self._max_alpha_limit = max_alpha_limit
        self._score_floor = score_floor
        self._last_candidates = deque(maxlen=candidate_history)
        self._logger = logging.getLogger(__name__)

    def fetch_candidate(self, task_type: str | None = None) -> Optional[Miner]:
        records = self._repository.fetch_candidate_records(task_type=task_type)
        logger = self._logger

        candidates: list[Tuple[Miner, float]] = []
        for miner, scores_mapping in records:
            stake_component = max(0, min(miner.alpha_stake, self._max_alpha_limit))
            score_multiplier = self._score_multiplier(miner, scores_mapping)
            candidates.append((miner, stake_component * score_multiplier))

        if not candidates:
            logger.warning(
                "miner_selection.no_candidates task_type=%s",
                task_type or "<any>",
            )
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

    def get_last_candidates(self) -> list[Tuple[datetime, Miner]]:
        return list(self._last_candidates)

    def _score_multiplier(self, miner: Miner, payload: Optional[Mapping[str, Any]]) -> float:
        if getattr(miner, "failed_audits", 0):
            return 0.0
        if not payload:
            return 1.0

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
        if hasattr(miner, "model_dump"):
            return Miner(**miner.model_dump())  # type: ignore[arg-type]
        if hasattr(miner, "dict"):
            return Miner(**miner.dict())  # type: ignore[attr-defined]
        return Miner(**dict(miner))

    def _record_candidate(self, miner: Miner) -> None:
        try:
            clone = self._clone_miner(miner)
        except Exception:  # pragma: no cover - defensive guard
            clone = miner
        self._last_candidates.append((datetime.now(timezone.utc), clone))


__all__ = ["MinerSelectionService"]
