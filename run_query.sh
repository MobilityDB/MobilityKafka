#!/usr/bin/env bash

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
banner()  { echo -e "\n${BOLD}${CYAN}═══════════════════════════════════════════${RESET}"; \
            echo -e "${BOLD}${CYAN}  $*${RESET}"; \
            echo -e "${BOLD}${CYAN}═══════════════════════════════════════════${RESET}\n"; }

declare -A QUERY_CLASS=(
    [ais1]="AISData_Queries.Query1_Main"
    [ais2]="AISData_Queries.Query2_Main"
    [ais3]="AISData_Queries.Query3_Main"
    [ais4]="AISData_Queries.Query4_Main"
    [ais5]="AISData_Queries.Query5_Main"
    [ais6]="AISData_Queries.Query6_Main"
    [ais7]="AISData_Queries.Query7_Main"
    [ais8]="AISData_Queries.Query8_Main"
    [ais9]="AISData_Queries.Query9_Main"
    [sncb1]="SNCBData_Queries.Query1_Main"
    [sncb2]="SNCBData_Queries.Query2_Main"
    [sncb3]="SNCBData_Queries.Query3_Main"
    [sncb4]="SNCBData_Queries.Query4_Main"
    [sncb5]="SNCBData_Queries.Query5_Main"
    [sncb6]="SNCBData_Queries.Query6_Main"
    [sncb7]="SNCBData_Queries.Query7_Main"
    [sncb8]="SNCBData_Queries.Query8_Main"
    [sncb9]="SNCBData_Queries.Query9_Main"
)

declare -A QUERY_DESC=(
    [ais1]="[AIS]  High-Risk Zone Proximity Monitoring"
    [ais2]="[AIS]  Brake System Monitoring"
    [ais3]="[AIS]  Trajectory Creation"
    [ais4]="[AIS]  Trajectory Creation in a Restricted Space"
    [ais5]="[AIS]  Trajectory Creation and High-Speed Alert"
    [ais6]="[AIS]  Positional Divergence for a Device"
    [ais7]="[AIS]  Global Closest Device Pairs (Top-k)"
    [ais8]="[AIS]  Trajectory Denoising — MEOS native EKF"
    [ais9]="[AIS]  Windowed Per-Device kNN Join"
    [sncb1]="[SNCB] High-Risk Zone Proximity Monitoring"
    [sncb2]="[SNCB] Brake System Monitoring"
    [sncb3]="[SNCB] Trajectory Creation"
    [sncb4]="[SNCB] Trajectory Creation in a Restricted Space"
    [sncb5]="[SNCB] Trajectory Creation and High-Speed Alert"
    [sncb6]="[SNCB] Positional Divergence for a Device"
    [sncb7]="[SNCB] Global Closest Device Pairs (Top-k)"
    [sncb8]="[SNCB] Trajectory Denoising — MEOS native EKF"
    [sncb9]="[SNCB] Windowed Per-Device kNN Join"
)

declare -A QUERY_DOCKERFILE=(
    [ais1]="Dockerfile"   [ais2]="Dockerfile"   [ais3]="Dockerfile"
    [ais4]="Dockerfile"   [ais5]="Dockerfile"   [ais6]="Dockerfile"
    [ais7]="Dockerfile"   [ais8]="Dockerfile_q8_meos_kalman"
    [ais9]="Dockerfile"
    [sncb1]="Dockerfile"  [sncb2]="Dockerfile"  [sncb3]="Dockerfile"
    [sncb4]="Dockerfile"  [sncb5]="Dockerfile"  [sncb6]="Dockerfile"
    [sncb7]="Dockerfile"  [sncb8]="Dockerfile_q8_meos_kalman"
    [sncb9]="Dockerfile"

)

# ── Usage ─────────────────────────────────────────────────────────────────────
QUERY_KEY="${1:-}"
if [[ -z "$QUERY_KEY" || ! -v "QUERY_CLASS[$QUERY_KEY]" ]]; then
  error "Usage : $(basename "$0") <query_key>"
  echo ""
  echo "Available queries :"
  for k in $(echo "${!QUERY_DESC[@]}" | tr ' ' '\n' | sort); do
    echo -e "  ${BOLD}${k}${RESET}  — ${QUERY_DESC[$k]}"
  done
  exit 1
fi

CLASS="${QUERY_CLASS[$QUERY_KEY]}"
DESC="${QUERY_DESC[$QUERY_KEY]}"
BASE_DOCKERFILE="${QUERY_DOCKERFILE[$QUERY_KEY]}"
IS_Q8=false
[[ "$QUERY_KEY" == "ais8" || "$QUERY_KEY" == "sncb8" ]] && IS_Q8=true

banner "Query $QUERY_KEY — $DESC"

# ── Requirements ──────────────────────────────────────────────────────────────
info "Verifying requirements..."

# Docker
if ! command -v docker &>/dev/null; then
  error "docker is not installed or missing in PATH."
  exit 1
fi

# docker compose (v2) or docker-compose (v1)
if docker compose version &>/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
elif command -v docker-compose &>/dev/null; then
  COMPOSE_CMD="docker-compose"
else
  error "Neither 'docker compose' nor 'docker-compose' are available."
  exit 1
fi
success "Docker OK  (compose: $COMPOSE_CMD)"

# Working directory : pom.xml required
if [[ ! -f "pom.xml" ]]; then
  error "pom.xml not found. Run this script from your project root."
  exit 1
fi

# Dockerfile source
if [[ ! -f "$BASE_DOCKERFILE" ]]; then
  error "Dockerfile '$BASE_DOCKERFILE' not found."
  exit 1
fi
success "Dockerfile: $BASE_DOCKERFILE"

# Maven
if ! command -v mvn &>/dev/null; then
  error "mvn is not install or missing in the PATH."
  exit 1
fi
success "Maven OK"

# Ignoring query8 files during the mvn build
Q8_STASHED=()

stash_q8_files() {
  while IFS= read -r -d '' f; do
    mv "$f" "${f}.bak"
    Q8_STASHED+=("$f")
    warn "Temporarily excluded from the compilation : $f"
  done < <(grep -rl --include="*.java" -Z "temporal_ext_kalman_filter" src/ 2>/dev/null || true)
}

restore_q8_files() {
  for f in "${Q8_STASHED[@]:-}"; do
    [[ -f "${f}.bak" ]] && mv "${f}.bak" "$f"
  done
}

trap 'restore_q8_files; rm -f "$TMP_DOCKERFILE" 2>/dev/null || true' EXIT

echo ""
info "Step 1/3: Build Maven (mvn clean package -DskipTests) ..."

if [[ "$QUERY_KEY" != "ais8" ]]; then
  info "Temporarily masking Query8 files (temporal_ext_kalman_filter not available before the patch)..."
  stash_q8_files
  if [[ ${#Q8_STASHED[@]} -eq 0 ]]; then
    warn "No Query8 files found to mask."
  fi
fi

if ! mvn clean package -DskipTests -DqueryMainClass="${CLASS}" -q; then
  error "Maven build failed."
  exit 1
fi

# Restoring the query8 files after the mvn build
restore_q8_files
Q8_STASHED=()

success "Build Maven done."

# Finding the JAR
JAR_PATH=""
for candidate in \
    "target/flink-kafka2postgres-1.0-SNAPSHOT.jar" \
    "jar/JMEOS-fat.jar" \
    target/*.jar jar/*.jar; do
  if [[ -f "$candidate" ]]; then
    JAR_PATH="$candidate"
    break
  fi
done

if [[ -z "$JAR_PATH" ]]; then
  error "No JAR found after building maven (target/ & jar/ have been verified)."
  exit 1
fi
success "JAR detected : $JAR_PATH"


# ── Step 1: Build Docker image with the right CMD ─────────────────────────────
echo ""
info "Step 1/2: Building Docker image..."
info "  Class targeted : ${CLASS}"
info "  Dockerfile     : ${BASE_DOCKERFILE}"

if ! grep -q "^CMD" "${BASE_DOCKERFILE}"; then
  error "No CMD found in ${BASE_DOCKERFILE}."
  exit 1
fi

# Patch the CMD line in the Dockerfile to point to the right main class.
# The CMD ends with "SomePackage.QueryX_Main"] — replace just that class token.
TMP_DF=$(mktemp /tmp/Dockerfile.query.XXXXXX)
trap 'rm -f "$TMP_DF" 2>/dev/null || true' EXIT
sed "s@\"[a-zA-Z_]*Queries\.[A-Za-z0-9_]*\"\]@\"${CLASS}\"]@g" "${BASE_DOCKERFILE}" > "${TMP_DF}"

if ! grep -q "${CLASS}" "${TMP_DF}"; then
  error "The patch failed: '${CLASS}' absent from the buffer Dockerfile."
  error "CMD found : $(grep '^CMD' ${BASE_DOCKERFILE})"
  rm -f "${TMP_DF}"; exit 1
fi

PATCHED_CMD=$(grep "^CMD" "${TMP_DF}")
success "Patched CMD: ${PATCHED_CMD}"

info "Building Docker image 'query-app'..."
if ! docker build -f "${TMP_DF}" --build-arg QUERY_MAIN_CLASS="${CLASS}" -t query-app .; then
  error "Docker build failed."
  rm -f "${TMP_DF}"; exit 1
fi
rm -f "${TMP_DF}"

BUILT_CMD=$(docker inspect --format='{{json .Config.Cmd}}' query-app 2>/dev/null || echo "")
if [[ "${BUILT_CMD}" != *"${CLASS}"* ]]; then
  error "Built image does not contain '${CLASS}' in its CMD."
  error "CMD detected: ${BUILT_CMD}"
  exit 1
fi
success "Image 'query-app' built."
info  "  CMD: ${BUILT_CMD}"

# ── Step 2: docker compose up ─────────────────────────────────────────────────
echo ""
info "Step 2/2: Running docker compose..."

_cleanup() {
  echo ""
  warn "Stopping..."
  $COMPOSE_CMD down --remove-orphans 2>/dev/null || true
  success "Docker compose stopped."
  exit 0
}
trap '_cleanup' INT TERM

info "Stopping the existing containers..."
$COMPOSE_CMD down --remove-orphans 2>/dev/null || true

echo ""
info "Running with ${COMPOSE_CMD} up --force-recreate ..."
info "Active class : ${CLASS}"
echo -e "${BOLD}─────────────────────────────────────────────────────${RESET}"

${COMPOSE_CMD} up --force-recreate