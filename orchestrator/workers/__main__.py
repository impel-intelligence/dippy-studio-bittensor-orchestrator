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
        choices=("metagraph", "score", "audit", "audit-seed", "audit-check", "audit-broadcast", "seed-requests", "all"),
        help=(
            "Which worker to execute (use 'audit-seed'/'audit-check' for split audit workflows, "
            "'audit-broadcast' to send a reference job to all miners, "
            "'seed-requests' to replay the latest audit job across miners, or 'all' to run metagraph + score sequentially)"
        ),
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
    parser.add_argument(
        "--audit-apply",
        dest="audit_apply",
        action="store_true",
        help="Allow the audit check runner to persist miner validity changes (defaults to dry run)",
    )
    parser.add_argument(
        "--audit-preview",
        dest="audit_preview",
        action="store_true",
        help="For audit-seed: preview candidates and stop before creating audit jobs",
    )
    parser.add_argument(
        "--audit-job-type",
        dest="audit_job_type",
        default="img-h100_pcie",
        help="For audit-seed: filter jobs by job_type (default: img-h100_pcie)",
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
    if "metagraph" in targets:
        logger.info(
            "runner.metagraph.start config=%s database_url=%s",
            args.config_path or "<default>",
            args.database_url or "<default>",
        )

    try:
        run_targets(
            targets,
            config_path=args.config_path,
            database_url=args.database_url,
            trace_hotkeys=trace_hotkeys or None,
            audit_apply_changes=args.audit_apply,
            audit_seed_preview=args.audit_preview,
            audit_seed_job_type=args.audit_job_type,
        )
    except Exception:
        logger.exception("runner.failed targets=%s", ",".join(targets))
        return 1

    logger.info("runner.complete targets=%s", ",".join(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())
