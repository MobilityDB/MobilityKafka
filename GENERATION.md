# MobilityKafka generation — the canonical per-binding generator policy

This document is the contract for how MobilityKafka is generated, under the ecosystem-wide
per-binding generator policy.

## The policy (ecosystem-wide)

Every MobilityDB language/surface binding is a **pure projection of the MEOS-API catalog**,
and **each binding owns its own generator, in its own repo**, in a canonical layout. The
single source of truth is the **catalog** (`MEOS-API/output/meos-idl.json`, generated from
the MEOS C headers). A binding is an independent, plug-and-play module that owns its
generation.

Each binding repo satisfies the same invariants: in-repo generator; own
`tools/pin/compose-order.txt`; pinned catalog/jar input; thin language projection; full
automation toward a zero-hand-written surface (generate-then-retire; the last green-CI
version is the equivalence probe).

## MobilityKafka scope: generated MEOS facades over the JMEOS surface

MobilityKafka is a **consumer** binding: it binds the **JMEOS jar** (the JVM FFI projection
of the catalog), not MEOS-API directly. Its generator **`tools/codegen_facades.py`** (the
same generator as MobilityFlink) reads the bundled JMEOS raw-FFI surface and emits the
`org.mobilitydb.meos.MeosOps*` 1:1 forwarder facades the `kafka-streams-app` consumes. The
facades are a *consumer* projection (they live here, not in JMEOS).

## Generate-then-retire — the green-CI version is the probe

Hand-written facades/glue are replaced by the generated forwarders **family by family,
never wipe-first**: regenerate, build green, **prove generated ⊇ hand** against the **last
green-CI version** (the test suite + the benchmark), then retire the hand path. The
generated facades are committed and regenerated on a pin bump.

## Pinning

The bundled JMEOS jar is built via `build-jmeos.sh` from a deliverable-PR head (never
committed as a binary), and the `libmeos.so` it links is built from the **same MobilityDB
pin** the JMEOS surface was generated against (surface-match, else runtime symbol faults).
That pin is the *catalog/surface* input; MobilityKafka's own `tools/pin/compose-order.txt`
governs *this repo's* PR accumulate.
