#!/usr/bin/env bash
# Runs Query 6 - Positional Divergence for a Device (AIS dataset)
# Usage : ./run_ais6.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" ais6 "$@"