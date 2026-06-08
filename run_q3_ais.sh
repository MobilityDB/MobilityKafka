#!/usr/bin/env bash
# Runs Query 3 - Trajectory Creation (AIS dataset)
# Usage : ./run_ais3.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" ais3 "$@"
