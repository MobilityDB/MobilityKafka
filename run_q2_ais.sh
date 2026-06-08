#!/usr/bin/env bash
# Runs Query 2 - Brake System Monitoring (AIS dataset)
# Usage : ./run_ais2.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" ais2 "$@"