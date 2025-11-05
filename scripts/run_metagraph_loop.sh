#!/usr/bin/env bash
set -euo pipefail

# Loop `just run-metagraph-once` every 20 minutes from repo root.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

while true; do
    timestamp="$(date -Is)"
    echo "${timestamp} starting run-metagraph-once"
    if ! just run-metagraph-once; then
        echo "${timestamp} run-metagraph-once failed" >&2
    fi
    sleep $((20 * 60))
done
