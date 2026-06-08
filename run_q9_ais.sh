#!/usr/bin/env bash
# Runs Query 9 - Windowed Per-Device kNN Join (AIS dataset)
# Usage : ./run_ais9.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" ais9 "$@"
