#!/usr/bin/env bash
# Runs Query 1 - High-Risk Zone Proximity Monitoring (AIS dataset)
# Usage : ./run_ais1.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" ais1 "$@"