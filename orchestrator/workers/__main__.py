from __future__ import annotations

import argparse
import logging
import re
import sys
from typing import Sequence

from orchestrator.workers import run_targets


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run orchestrator background tasks on demand",
    )
    parser.add_argument(
        "target",
        choices=("metagraph", "score", "all"),
        help="Which worker to execute (or 'all' to run both sequentially)",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        help="Optional explicit path to orchestrator config file",
    )
    parser.add_argument(
        "--database-url",
        dest="database_url",
        help="Override the database URL (defaults to configuration/env)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Increase logging verbosity for the CLI wrapper",
    )
    parser.add_argument(
        "--hotkey",
        dest="trace_hotkeys",
        action="append",
        help="Optional hotkey to trace through the score ETL (repeatable)",
    )
    return parser.parse_args(argv)


def _normalize_hotkeys(values: Sequence[str] | None) -> list[str]:
    """Sanitize CLI hotkey inputs and split comma/space separated values."""

    if not values:
        return []

    normalized: list[str] = []
    for raw in values:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue

        if "=" in text:
            text = text.split("=", 1)[1].strip()
        if not text:
            continue

        parts = re.split(r"[,\s]+", text)
        for part in parts:
            candidate = part.strip()
            if candidate:
                normalized.append(candidate)

    return normalized


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger("orchestrator.workers")

    targets = ["metagraph", "score"] if args.target == "all" else [args.target]
    trace_hotkeys = _normalize_hotkeys(args.trace_hotkeys)

    logger.info("runner.start targets=%s", ",".join(targets))

    try:
        run_targets(
            targets,
            config_path=args.config_path,
            database_url=args.database_url,
            trace_hotkeys=trace_hotkeys or None,
        )
    except Exception:
        logger.exception("runner.failed targets=%s", ",".join(targets))
        return 1

    logger.info("runner.complete targets=%s", ",".join(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())
