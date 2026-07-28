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

The chain is reproduced from source, so the repository carries no jar or `libmeos.so`:

```
MobilityDB @ master
  → provision-meos (MEOS-API/run.py + cmake -DMEOS=ON -DALL) → meos-idl.json + libmeos.so
  → JMEOS main  (mvn install)                                → JMEOS.jar → org.jmeos:meos:1.0
  → tools/codegen_jvm.py --engine kafka  (full jar surface)  → org.mobilitydb.meos.MeosOps* facades
  → binding  (mvn test)
```

CI derives the catalog + all-families `libmeos.so` from upstream MobilityDB master through the
shared `provision-meos` action, stages the catalog to `tools/meos-idl.json`, and builds the
JMEOS jar from JMEOS `main` against that libmeos, installing it as `org.jmeos:meos:1.0` — the
same jar coordinates and build steps MobilitySpark and MobilityFlink use. Tracking master (not a
pinned commit) keeps the source, the catalog, the jar and the libmeos the tests load all moving
together, so the generated facades can never drift from the surface they run against.

## Regenerating by hand

CI performs the steps below via `provision-meos`. To run them yourself you need a JDK, Maven,
CMake and the MEOS build dependencies.

**1. Derive libmeos and the catalog from MobilityDB master.** Both come from one commit; see
`MEOS-API/GENERATION.md` for the commands:

```bash
MDB=~/src/MobilityDB                     # checkout at the commit you are deriving from
MEOSAPI=~/src/MEOS-API
cmake -S "$MDB" -B "$MDB/build" -DCMAKE_BUILD_TYPE=Release -DMEOS=ON -DALL=ON
cmake --build "$MDB/build" -j"$(nproc)"
cmake --install "$MDB/build" --prefix "$MDB/.prefix"
cd "$MEOSAPI" && MDB_SRC_ROOT="$MDB" python3 run.py "$MDB/.prefix/include"
```

**2. Build the JMEOS jar against that catalog and install it into the local Maven repository**
under the coordinates this build resolves — `org.jmeos:meos:1.0`, the same as MobilitySpark and
MobilityFlink:

```bash
cd ~/src/JMEOS                            # JMEOS main
CATALOG="$MEOSAPI/output/meos-idl.json" LIBMEOS="$MDB/.prefix/lib/libmeos.so" \
  tools/regen-from-catalog.sh
mvn install:install-file -Dfile=jar/JMEOS.jar \
  -DgroupId=org.jmeos -DartifactId=meos -Dversion=1.0 -Dpackaging=jar
```

**3. Stage the catalog and build.** `tools/meos-idl.json` is derived, not committed:

```bash
cd ~/src/MobilityKafka
cp "$MEOSAPI/output/meos-idl.json" tools/meos-idl.json
mvn -Dmeos.lib.dir="$MDB/.prefix/lib" clean test
```

`generate-sources` runs `tools/codegen_jvm.py --engine kafka --catalog ../tools/meos-idl.json
--jar <the installed jar> --out target/generated-facades`, so the `MeosOps*` facades are
regenerated by the build itself. `meos.lib.dir` is where the tests find `libmeos.so`.
