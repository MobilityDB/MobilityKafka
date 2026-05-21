# Kafka Streams wirings for the generated MEOS facades

This package supplies thin, generic Kafka-Streams-DSL wrappers around
the generated `org.mobilitydb.kafka.meos.MeosOps*` facades, organized
per **streaming tier** (per
`tools/codegen/meos-ops-manifest.json` + `tools/codegen/meos-ops-free-manifest.json`).

Mirrors the MobilityFlink wirings package
([`org.mobilitydb.flink.meos.wirings`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/src/main/java/org/mobilitydb/flink/meos/wirings))
on the Kafka side; the tier classification is identical, the engine
realizations differ (Kafka Streams DSL → lambda-shaped wirings; Flink
DataStream → class-shaped wirings).

| Tier | Wiring class(es) here | Method count (v4 baseline) |
|---|---|---:|
| `stateless` | [`MeosStatelessOps`](MeosStatelessOps.java) — `predicate(...)` / `intPredicate(...)` / `mapper(...)` factories returning serializable `Predicate` / `ValueMapper` for `KStream.filter` / `.mapValues` | 804 |
| `bounded-state` | [`MeosBoundedStateProcessor`](MeosBoundedStateProcessor.java) — `Processor` with `KeyValueStore<K, byte[]>`; per-key MEOS-handle state crosses the operator boundary as `byte[]` (MEOS-WKB or MEOS-WKT) so Kafka Streams' changelog-replay / rebalance / state-rebuild paths work correctly | 797 |
| `windowed` | [`MeosWindowedAggregator`](MeosWindowedAggregator.java) — `initializer(...)` + `aggregator(...)` factories for `KStream.groupByKey().windowedBy(...).aggregate(...)`; pairs with `Materialized.with(...)` for the window state store | 161 |
| `cross-stream` | [`MeosCrossStreamJoiner`](MeosCrossStreamJoiner.java) — `joiner(...)` factory wrapping a serializable `ValueJoiner` for `KStream.join(other, ValueJoiner, JoinWindows)`; same-key pairing, time-bounded match window | 140 |
| `io-meta` | covered by `MeosStatelessOps.mapper(...)` (no state, no window) | 195 |
| `sequence-only` | inherently non-streamable — no wiring | 14 |

**Cumulative coverage**: same as the Flink side — **2,097 of 2,097
emitted methods (100%)** wirable through 5 generic wirings classes
without per-method registration.

## Why DSL-shaped (lambda-driven) wirings rather than class-shaped

Kafka Streams' DSL is lambda-first: `KStream.mapValues((k, v) -> …)`,
`KStream.filter((k, v) -> …)`, `KStream.groupByKey().aggregate(init, agg, …)`.
Most tiers can be wired with a single serializable lambda; only
`bounded-state` requires a full `Processor` class for state-store
binding. The wirings here reflect that asymmetry — small static-helper
factory classes for the lambda-shaped tiers, and a real
`Processor` class only for `bounded-state`.

Adopters who want a class-shaped wiring (matching the Flink layout
for cross-binding parity) can subclass any of these helpers; the
serializable functional interfaces (`MeosPredicate`, `MeosMapper`,
`MeosStepFn`, `MeosAggregator`, etc.) are public.

## How a generated MEOS call becomes a Kafka Streams operator

```java
// 1. Pick the generated MeosOps method (Javadoc tier marker tells you which wiring)
boolean overlap = MeosOpsFreeCore.overlaps_tbox_tbox(boxA, boxB);  // tier = stateless

// 2. Wrap with the matching wiring factory
KStream<String, TboxPair> overlapping = stream.filter(
    MeosStatelessOps.intPredicate(
        (key, pair) -> MeosOpsFreeCore.overlaps_tbox_tbox(pair.a, pair.b)));
```

`MEOS_AVAILABLE` is probed once per JVM by the shared
`org.mobilitydb.kafka.meos.MeosOpsRuntime` static initializer (the same
runtime the codegen package uses). When unavailable, every generated
method throws `UnsupportedOperationException` with a clear message —
the wirings layer doesn't have to handle that itself.

## End-to-end runnable demo

[`demo/MeosWiringsDemoTopology`](demo/MeosWiringsDemoTopology.java) is
a Kafka Streams topology that composes all four tier wirings in a
single pipeline:

1. **Source**: `vehicle-events` topic — `(regionId, tboxWKT)` records.
2. **Stateless filter** (`MeosStatelessOps.predicate`) — drop events for regions outside the interest set.
3. **Bounded-state processor** (`MeosBoundedStateProcessor`) — per-region running tbox union, state stored as MEOS-WKT bytes in a `KeyValueStore<Integer, byte[]>`.
4. **Windowed aggregator** (`MeosWindowedAggregator.aggregator`) — per-region 30s tumbling window, accumulator is the latest running-union WKT.
5. **Cross-stream joiner** (`MeosCrossStreamJoiner.joiner`) — join the windowed vehicle aggregates with a `region-queries` topic on shared `regionId`, ±1m time bound; emit on tbox overlap.
6. **Sink**: `overlap-output` topic.

Run with:

```bash
mvn -q exec:java \
    -Dexec.mainClass=org.mobilitydb.kafka.meos.wirings.demo.MeosWiringsDemoTopology \
    -Dmobilityflink.meos.enabled=true
```

The demo uses `TopologyTestDriver` (kafka-streams-test-utils) — no
Kafka broker required. The `main()` method always prints the topology
description; when `MEOS_AVAILABLE`, it also instantiates the
`TopologyTestDriver` to validate the topology end-to-end at startup.

## Coexistence with `berlinmod.MEOSBridge`

`berlinmod.MEOSBridge` (introduced on `feat/jmeos-bridge-swap`) is the
hand-written, BerlinMOD-scoped bridge for the 9-query streaming-form
parity matrix — high-level and query-shaped. The wirings here are
low-level and catalog-shaped — applicable to any of the ~1,800
streamable generated methods, not just the BerlinMOD-9 subset. Both
share the same `MEOS_AVAILABLE` discipline (via `MeosOpsRuntime`) and
the same `functions.GeneratedFunctions` delegation.
