#!/usr/bin/env bash
# Runs Query 5 - Trajectory Creation and High-Speed Alert (AIS dataset)
# Usage : ./run_ais5.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" ais5 "$@"