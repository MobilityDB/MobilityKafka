# MobilityKafka generation — the canonical per-binding generator policy

This document is the contract for how MobilityKafka is generated, under the ecosystem-wide
per-binding generator policy.

## The policy (ecosystem-wide)

Every MobilityDB language/surface binding is a **pure projection of the MEOS-API catalog**,
and the JVM bindings **share one generator**. The single source of truth is the **MEOS C
API** (via the MEOS-API catalog `meos-idl.json`, generated from the MEOS headers). A binding
is an independent, plug-and-play module that owns its generation.

Each binding repo satisfies the same invariants: the shared generator; catalog/jar input
derived in CI from upstream MobilityDB master; thin language projection; no committed native
binaries; no committed generated sources.

## MobilityKafka scope: generated MEOS facades over the JMEOS surface

MobilityKafka is a **consumer** binding: it binds the **JMEOS jar** (the JVM FFI projection
of the catalog), not MEOS-API directly. Its generator is the shared
**`tools/codegen_jvm.py --engine kafka`**, the single generator vendored identically by every
JVM binding (MobilitySpark, MobilityFlink, MobilityKafka); the `flink` and `kafka` engines
emit the `org.mobilitydb.meos.MeosOps*` 1:1 forwarder facades the Kafka Streams app consumes.
The facades are a *consumer* projection — they live here, not in JMEOS, so the JMEOS FFI line
and the facade line do not diverge.

## Full surface, grouped by the catalog object model

`codegen_jvm.py --engine kafka` emits a facade for **every** function on the bundled JMEOS
`functions.GeneratedFunctions` surface, grouped by the MEOS-API catalog object model: one
`MeosOps<Class>` per object-model class plus one `MeosOpsFree<Header>` per source header for
the free functions, with a shared `MeosOpsRuntime` that probes libmeos once per JVM. Each
forwarder carries a runtime guard: functions whose catalog return type is sequence-typed
(build a whole `TSequence`/`SeqSet`, inherently non-streamable) throw
`UnsupportedOperationException`; all others forward to `GeneratedFunctions` behind the
`MEOS_AVAILABLE` probe. The class/role/header are read straight from the catalog's
`objectModel`, and the sequence check from `returnType.canonical` — no separate classifier.

The `MeosOps*` facades are emitted at build time and are **not committed**: Maven
`generate-sources` runs `tools/codegen_jvm.py --engine kafka` into `target/generated-facades`,
and `build-helper` adds it as a source root. The sole hand-written class under
`org.mobilitydb.meos` is `MeosSetSetJoin`.

## The build chain — no committed binaries

`build-jmeos.sh` reproduces the native/JVM chain, so the repository carries no jar or
`libmeos.so`:

```
MobilityDB @ master
  → provision-meos (MEOS-API/run.py + cmake -DMEOS=ON -DALL) → meos-idl.json + libmeos.so
  → build-jmeos.sh: JMEOS main                               → JMEOS.jar → com.mobilitydb:jmeos:1.4.0
  → tools/codegen_jvm.py --engine kafka  (full jar surface)  → org.mobilitydb.meos.MeosOps* facades
  → kafka-streams-app  (mvn test)
```

CI derives the catalog + all-families `libmeos.so` from upstream MobilityDB master through
the shared `provision-meos` action, stages the catalog to `tools/meos-idl.json`, and lets
`build-jmeos.sh` build the JMEOS jar from JMEOS `main` against that libmeos. Tracking master
(not a pinned commit) keeps the source, the catalog, the jar and the libmeos the tests load
all moving together, so the generated facades can never drift from the surface they run
against.

## Regenerating by hand

CI performs the steps below via `provision-meos` and `build-jmeos.sh`. To run them yourself
you need a JDK, Maven, CMake and the MEOS build dependencies.

**1. Derive libmeos and the catalog from MobilityDB master.** Both come from one commit; see
`MEOS-API/GENERATION.md` for the two commands:

```bash
MDB=~/src/MobilityDB                     # checkout at the commit you are deriving from
MEOSAPI=~/src/MEOS-API
cmake -S "$MDB" -B "$MDB/build" -DCMAKE_BUILD_TYPE=Release -DMEOS=ON -DALL=ON
cmake --build "$MDB/build" -j"$(nproc)"
cmake --install "$MDB/build" --prefix "$MDB/.prefix"
cd "$MEOSAPI" && MDB_SRC_ROOT="$MDB" python3 run.py "$MDB/.prefix/include"
```

**2. Stage the catalog and build the JMEOS jar.** `tools/meos-idl.json` is derived, not
committed. `build-jmeos.sh` builds the jar from JMEOS `main` and installs it into the local
Maven repository as `com.mobilitydb:jmeos:1.4.0`, the coordinates this build resolves. It
reuses an already-built libmeos rather than building its own — `INSTALLED_LIBMEOS` points at
one, defaulting to `/usr/local/lib/libmeos.so` — and copies it into `kafka-streams-app/lib/`:

```bash
cd ~/src/MobilityKafka
cp "$MEOSAPI/output/meos-idl.json" tools/meos-idl.json
INSTALLED_LIBMEOS="$MDB/.prefix/lib/libmeos.so" ./build-jmeos.sh
```

Without `INSTALLED_LIBMEOS` the script clones MobilityDB at `MOBILITYDB_REF` (default
`master`) and builds libmeos itself, which is the same derivation done twice.

**3. Build and run the tests**, resolving the library from where step 2 placed it:

```bash
cd kafka-streams-app
LD_LIBRARY_PATH="$PWD/lib" mvn test
```

`generate-sources` runs `tools/codegen_jvm.py --engine kafka --catalog ../tools/meos-idl.json
--jar <the installed jar> --out target/generated-facades`, so the `MeosOps*` facades are
regenerated by the build itself. `LD_LIBRARY_PATH` is how the tests find `libmeos.so`.
