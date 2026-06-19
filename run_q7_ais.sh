#!/usr/bin/env bash
# Runs Query 7 - Global Closest Device Pairs (Top-k) (AIS dataset)
# Usage : ./run_ais7.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" ais7 "$@"
