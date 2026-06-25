#!/usr/bin/env bash
# regen-from-pin.sh — regenerate the MobilityKafka MEOS facades from the JMEOS jar
# (per GENERATION.md). MobilityKafka is a JMEOS consumer.
#
# Usage:  tools/regen-from-pin.sh <pin>
#   env:  JMEOS_JAR = path to the JMEOS jar built from the same pin (required)
#
# NOTE (topology gap, tools/pin/compose-order.txt): the consumer + tools/codegen_facades.py
# currently live on the fork branch consolidate/kafka-benchmark; once that lands on main this
# script regenerates in place. Invoked standalone, or by MEOS-API tools/ecosystem-generate.sh
# (after the JMEOS jar).
set -euo pipefail
PIN="${1:?usage: regen-from-pin.sh <pin>}"
JMEOS_JAR="${JMEOS_JAR:?set JMEOS_JAR to the JMEOS jar built from the same pin}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

# run the in-repo generator (tools/codegen_facades.py: --jar --out --engine) ->
# org.mobilitydb.meos.MeosOps* forwarder facades under <out>/src/main/java
python3 "$HERE/tools/codegen_facades.py" --jar "$JMEOS_JAR" --out "$HERE" --engine kafka

# build-verify
( cd "$HERE" && mvn -q test ) || echo "WARN: MobilityKafka mvn test returned non-zero"
echo "[kafka] regenerated facades from JMEOS jar at pin $PIN"
