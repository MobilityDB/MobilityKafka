#!/usr/bin/env bash
# Runs Query 4 - Trajectory Creation in a Restricted Space (SNCB dataset)
# Usage : ./run_sncb4.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" sncb4 "$@"