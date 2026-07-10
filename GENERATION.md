# MobilityKafka generation — the canonical per-binding generator policy

This document is the contract for how MobilityKafka is generated, under the ecosystem-wide
per-binding generator policy.

## The policy (ecosystem-wide)

Every MobilityDB language/surface binding is a **pure projection of the MEOS-API catalog**,
and **each binding owns its own generator, in its own repo**, in a canonical layout. The
single source of truth is the **MEOS C API** (via the MEOS-API catalog `meos-idl.json`,
generated from the MEOS headers). A binding is an independent, plug-and-play module that
owns its generation.

Each binding repo satisfies the same invariants: in-repo generator; catalog/jar input from
a specific MobilityDB commit; thin language projection; no committed native binaries.

## MobilityKafka scope: generated MEOS facades over the JMEOS surface

MobilityKafka is a **consumer** binding: it binds the **JMEOS jar** (the JVM FFI projection
of the catalog), not MEOS-API directly. Its generator **`tools/codegen_facades.py`** reads
the JMEOS raw-FFI surface (intersected with the streaming-relevance baseline) and emits the
`org.mobilitydb.meos.MeosOps*` 1:1 forwarder facades the Kafka Streams app consumes
(`--engine kafka`). The facades are a *consumer* projection — they live here, not in JMEOS,
so the JMEOS FFI line and the facade line do not diverge.

## The build chain — no committed binaries

`build-jmeos.sh` reproduces the whole native/JVM chain, so the repository carries no jar or
`libmeos.so`:

```
MobilityDB @ tools/meos-source-commit.txt
  → build-jmeos.sh: cmake -DMEOS=ON            → libmeos.so   (families CBUFFER/NPOINT/POSE)
  → build-jmeos.sh: JMEOS main                 → JMEOS.jar    → installed com.mobilitydb:jmeos:1.4.0
  → tools/codegen_facades.py --engine kafka    → org.mobilitydb.meos.MeosOps* facades
  → kafka-streams-app  (mvn test)
```

`build-jmeos.sh` tracks upstream MobilityDB master and MobilityDB/JMEOS main (recorded as
immutable head SHAs, overridable via the environment). The `libmeos.so` it builds must come
from the **same commit** the JMEOS facade surface was generated against — surface-match, else
runtime symbol faults; only the symbols the app calls must be present (jnr resolves lazily).

## The streaming-relevance baseline (generator input)

`tools/codegen_facades.py` emits a facade only for functions in the **streaming-relevant**
tiers, read from `tools/baseline/streaming-relevance-baseline.json`. That baseline is itself
**generated and reproducible** — it is not hand-maintained. It is produced by
**`tools/classify_streaming_relevance.py`**, a deterministic classifier: the tier of a
function is decided purely by its name, its object-model role, and its number of temporal
parameters (zero per-function judgement), so the same MEOS catalog always yields the same
baseline.

To refresh the baseline (e.g. after bumping `tools/meos-source-commit.txt`): rebuild the
catalog at the tracked commit with MEOS-API, then

```
tools/regen_baseline.sh <path-to-meos-idl.json>
```

and commit the diff. Because the classifier is deterministic, an unchanged catalog
regenerates the baseline byte-for-byte.
