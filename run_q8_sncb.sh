#!/usr/bin/env bash
# Runs Query 8 - Trajectory Denoising — MEOS native EKF (SNCB dataset)
# Usage : ./run_sncb8.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/run_query.sh" sncb8 "$@"