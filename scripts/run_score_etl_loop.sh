#!/usr/bin/env bash
set -euo pipefail

# Loop `just run-score-etl-once` every 60 minutes from repo root.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

while true; do
    timestamp="$(date -Is)"
    echo "${timestamp} starting run-score-etl-once"
    if ! just run-score-etl-once; then
        echo "${timestamp} run-score-etl-once failed" >&2
    fi
    sleep $((60 * 60))
done
