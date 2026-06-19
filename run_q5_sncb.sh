#!/usr/bin/env bash
# Runs Query 5 - Trajectory Creation and High-Speed Alert (SNCB dataset)
# Usage : ./run_sncb5.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" sncb5 "$@"