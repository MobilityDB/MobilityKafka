#!/usr/bin/env bash
#
# build-jmeos.sh — build the JMEOS jar and the native libmeos.so from source and
# install them locally, so the repository never has to carry the binaries.
#
# This is the downstream generation chain for the JVM streaming tools:
#
#     MobilityDB/MEOS (deliverable PRs) -> JMEOS (FFI facade + jar) -> MobilityKafka
#
# (The MEOS-API meos-idl.json step is pre-materialized in the JMEOS branch's
# committed codegen/input/meos-idl.json, so this script only has to build the
# two endpoints.)
#
# What it does:
#   1. Clones MobilityDB at the pinned ref and builds libmeos.so (cmake -DMEOS=ON).
#   2. Clones JMEOS at the pinned ref, drops libmeos.so in, and builds JMEOS.jar.
#   3. Registers the jar in the local Maven repository via
#      `mvn install:install-file` under the coordinates the kafka-streams-app
#      pom depends on (com.mobilitydb:jmeos:1.4.0 by default).
#   4. Copies libmeos.so into kafka-streams-app/lib/ for the test/runtime
#      LD_LIBRARY_PATH.
#
# After running this once, `cd kafka-streams-app && mvn test` resolves JMEOS as
# an ordinary dependency — no committed jar/so required.
#
# The refs below track upstream MobilityDB master and MobilityDB/JMEOS main — the
# surfaces this project is generated against. They are recorded as immutable head
# SHAs (overridable env vars) so a build is reproducible; bump them to the current
# master/main tips when refreshing the generated surface (see GENERATION.md).
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Pinned sources (override any of these via the environment).
# ---------------------------------------------------------------------------
# MobilityDB master — the surface the JMEOS facade is generated against, so the
# built libmeos.so matches the facade catalog. Overridable via the environment.
MOBILITYDB_REPO="${MOBILITYDB_REPO:-https://github.com/MobilityDB/MobilityDB.git}"
MOBILITYDB_REF="${MOBILITYDB_REF:-d984d747acc1fcdee895ebaf7517912d596ea598}"  # master 2026-07-10

# JMEOS main — functions.GeneratedFunctions (built at build-time from the committed
# catalog) plus the org.mobilitydb.meos.MeosOps* facades.
JMEOS_REPO="${JMEOS_REPO:-https://github.com/MobilityDB/JMEOS.git}"
JMEOS_REF="${JMEOS_REF:-5275e7d44cf9a62b731b2c3c2c9aa4ccebafc604}"  # main 2026-07-10

# Maven coordinates the jar is installed under (must match kafka-streams-app/pom.xml).
JMEOS_GROUP_ID="${JMEOS_GROUP_ID:-com.mobilitydb}"
JMEOS_ARTIFACT_ID="${JMEOS_ARTIFACT_ID:-jmeos}"
JMEOS_VERSION="${JMEOS_VERSION:-1.4.0}"

# ---------------------------------------------------------------------------
# Layout.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="${SCRIPT_DIR}/kafka-streams-app"
WORK_DIR="${WORK_DIR:-${SCRIPT_DIR}/.build-jmeos}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"

log() { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }

# ---------------------------------------------------------------------------
# Preconditions.
# ---------------------------------------------------------------------------
for tool in git cmake make mvn; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: '$tool' is required but not on PATH" >&2; exit 1; }
done

mkdir -p "${WORK_DIR}"

# clone_at <repo-url> <ref> <dest>
# Clones (or reuses) <dest> and checks out the exact <ref>. <ref> may be a tag,
# branch or commit SHA; PR-head SHAs are fetched from the pull ref namespace if
# they are not reachable from the default branches.
clone_at() {
  local repo="$1" ref="$2" dest="$3"
  if [ ! -d "${dest}/.git" ]; then
    log "Cloning ${repo}"
    git clone "${repo}" "${dest}"
  fi
  # Honor a changed repo URL on a reused checkout (e.g. fork -> upstream).
  git -C "${dest}" remote set-url origin "${repo}"
  git -C "${dest}" fetch --quiet --tags origin
  if ! git -C "${dest}" cat-file -e "${ref}^{commit}" 2>/dev/null; then
    # Not reachable from the default branches — fetch the exact ref directly, then
    # fall back to the PR-head namespace for an open-PR SHA.
    git -C "${dest}" fetch --quiet origin "${ref}" 2>/dev/null || \
    git -C "${dest}" fetch --quiet origin '+refs/pull/*/head:refs/remotes/origin/pr/*' || true
  fi
  log "Checking out ${ref}"
  # -f: a prior run copies libmeos.so into jmeos-core/src/, dirtying the tree; discard
  # such local changes so a reused checkout does not block on them.
  git -C "${dest}" -c advice.detachedHead=false checkout -f --quiet "${ref}"
}

# ---------------------------------------------------------------------------
# 1. Build libmeos.so from MobilityDB.
# ---------------------------------------------------------------------------
MDB_DIR="${WORK_DIR}/MobilityDB"
clone_at "${MOBILITYDB_REPO}" "${MOBILITYDB_REF}" "${MDB_DIR}"

# Enable the MEOS type families the streaming app exercises: circular buffers,
# network points (default ON) and geoposes (which auto-enables rigid geometries).
# The facade smoke tests link these symbols, so they must be in libmeos.so.
# H3 and POINTCLOUD are left OFF — the app does not use them and they require
# extra system libraries (libh3, libpointcloud); enable them via MEOS_CMAKE_ARGS
# if a downstream consumer ever needs them.
MEOS_CMAKE_ARGS="${MEOS_CMAKE_ARGS:--DCBUFFER=ON -DNPOINT=ON -DPOSE=ON}"

log "Building libmeos.so (MEOS=ON ${MEOS_CMAKE_ARGS})"
rm -rf "${MDB_DIR}/build"
cmake -S "${MDB_DIR}" -B "${MDB_DIR}/build" -DMEOS=ON ${MEOS_CMAKE_ARGS} >/dev/null
cmake --build "${MDB_DIR}/build" --target meos -j "${JOBS}"

LIBMEOS_SO="$(find "${MDB_DIR}/build" -name 'libmeos.so' -print -quit)"
[ -n "${LIBMEOS_SO}" ] || { echo "error: libmeos.so not produced by the MEOS build" >&2; exit 1; }
log "Built ${LIBMEOS_SO}"

# ---------------------------------------------------------------------------
# 2. Build JMEOS.jar against that libmeos.so.
# ---------------------------------------------------------------------------
JMEOS_DIR="${WORK_DIR}/JMEOS"
clone_at "${JMEOS_REPO}" "${JMEOS_REF}" "${JMEOS_DIR}"

# JMEOS' build bundles src/libmeos.so into the jar and JarLibraryLoader extracts it.
cp -f "${LIBMEOS_SO}" "${JMEOS_DIR}/jmeos-core/src/libmeos.so"

log "Building JMEOS.jar"
# FunctionsGenerator lives in the codegen module, which jmeos-core does not
# declare as a Maven dependency — so '-am' will not build it. Compile it first
# so jmeos-core's build-time facade generation can find it. Use
# 'maven.test.skip' (not 'skipTests'): the jmeos-core pom hardcodes
# <skipTests>false</skipTests>, which overrides -DskipTests but not this.
mvn -f "${JMEOS_DIR}/pom.xml" -q -pl codegen compile
mvn -f "${JMEOS_DIR}/pom.xml" -q -pl jmeos-core -am -Dmaven.test.skip=true package

JMEOS_JAR="${JMEOS_DIR}/jar/JMEOS.jar"
[ -f "${JMEOS_JAR}" ] || { echo "error: ${JMEOS_JAR} was not produced" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 3. Install the jar into the local Maven repository.
# ---------------------------------------------------------------------------
log "Installing ${JMEOS_GROUP_ID}:${JMEOS_ARTIFACT_ID}:${JMEOS_VERSION} into the local Maven repo"
mvn -q install:install-file \
  -Dfile="${JMEOS_JAR}" \
  -DgroupId="${JMEOS_GROUP_ID}" \
  -DartifactId="${JMEOS_ARTIFACT_ID}" \
  -Dversion="${JMEOS_VERSION}" \
  -Dpackaging=jar

# ---------------------------------------------------------------------------
# 4. Stage libmeos.so for the kafka-streams-app runtime (LD_LIBRARY_PATH).
# ---------------------------------------------------------------------------
mkdir -p "${APP_DIR}/lib"
cp -f "${LIBMEOS_SO}" "${APP_DIR}/lib/libmeos.so"

log "Done."
cat <<EOF

  Installed jar : ${JMEOS_GROUP_ID}:${JMEOS_ARTIFACT_ID}:${JMEOS_VERSION}
  Native library: ${APP_DIR}/lib/libmeos.so

  Build and test the app with:

      cd ${APP_DIR}
      mvn test

EOF
