# MEOS-API → MobilityKafka codegen

`org.mobilitydb.kafka.meos.MeosOps*` is a generated, tier-aware Java
facade over the MEOS public API. Two siblings of classes:

- `MeosOps<Class>` — one per MEOS object-model class (50 classes, 751 methods)
- `MeosOpsFree<Header>` — one per public MEOS header for free fns (6 headers, 1,346 methods)

Mirrors the same generator pair used on MobilityFlink (`codegen/flink-meos-ops`);
differs only in package path (`org.mobilitydb.kafka.meos`) and module
layout (`kafka-streams-app/` vs `flink-processor/`).

Each emitted method forwards to `functions.GeneratedFunctions.<name>(...)`
after probing the shared `MeosOpsRuntime.MEOS_AVAILABLE` flag. Method
Javadocs carry a tier marker — see the MobilityFlink companion for
the full tier vocabulary and Kafka Streams wiring shape per tier.

## Regeneration

Same recipe as MobilityFlink's `tools/codegen/README.md`; only the
output directory differs.
