# BerlinMOD streaming benchmark (Kafka Streams)

The Kafka-Streams counterpart of the MobilityFlink BerlinMOD benchmark covers
the 27 BerlinMOD-9 × 3-form cells (Q1–Q9 × continuous / windowed / snapshot)
with two harnesses, both over the same corpus and corpus-derived parameters, and
with the spatial predicates evaluating through MEOS (see
[`MEOSBridge`](../src/main/java/berlinmod/MEOSBridge.java)):

- [`BerlinMODBenchmark`](../src/main/java/berlinmod/BerlinMODBenchmark.java) runs
  each cell in isolation through a [`TopologyTestDriver`](../src/main/java/berlinmod/BerlinMODTopology.java)
  and reads its output cardinality — an in-process **correctness** harness.
- [`EmbeddedBrokerBenchmark`](../src/test/java/berlinmod/EmbeddedBrokerBenchmark.java)
  runs each cell as a real [`KafkaStreams`](../src/main/java/berlinmod/BerlinMODTopology.java)
  application against an in-process `EmbeddedKafkaCluster` (a genuine
  `KafkaServer` over the loopback network) — the **throughput** harness, the
  Flink-comparable analog of MobilityFlink's per-cell jobs.

## Corpus and parameters (regular with MobilityFlink)

The corpus is the real BerlinMOD instants (`--csv`, reprojected EPSG:3857→4326
through MEOS by [`BerlinMODCorpus`](../src/main/java/berlinmod/BerlinMODCorpus.java))
or a synthetic corpus. The per-query parameters (point `P`, region box, road
segment, points of interest, target vehicle ids) **and** the window/tick
granularity are derived from the corpus via `BerlinMODCorpus.derive` — the same
mechanism MobilityFlink uses — and threaded through `BerlinMODTopology.build(Params)`,
so the topology auto-scales to the corpus span instead of carrying a fixed
window/tick. The two stream bindings share one mechanism.

## Throughput harness

Each cell runs against its own fresh `EmbeddedKafkaCluster`, the true analog of
MobilityFlink's independent per-cell jobs: the corpus is produced once into the
input topic, the cell runs as a single-threaded `KafkaStreams` application, and
throughput is the events consumed divided by the wall-clock from streams start
until the application has read the whole input topic (its own
`records-consumed-total` metric reaches the input end offset) and its output has
gone idle. The trailing settle time is excluded from the wall; each consumed
record runs the cell's MEOS predicate, so this is the steady-state per-event
processing rate, directly comparable to the MobilityFlink figures.

Run from `kafka-streams-app/` after `../build-jmeos.sh` (which installs JMEOS into
the local Maven repository and stages `libmeos.so` under `lib/`). The test-scope
classpath that `mvn` reconstructs already carries JMEOS and the embedded broker, so
no jar is referenced by path:

```
CP=$(mvn -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout | tail -1)
LD_LIBRARY_PATH=lib java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
  -cp target/classes:target/test-classes:$CP \
  berlinmod.EmbeddedBrokerBenchmark --csv <berlinmod_instants.csv> [--max N] [--only Q3-continuous]
```

## Figures

Real BerlinMOD corpus (216,075 instants, 5 vehicles, ~11 days, EPSG:3857),
single-broker `EmbeddedKafkaCluster`, one stream thread per cell, Java 21,
16-core host; libmeos built with `-DMEOS/CBUFFER/NPOINT/POSE/RGEO=ON`.

| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |
|---|---:|---:|---:|---:|
| Q1-continuous | 216075 | 5 | 1899 | 113,785 |
| Q1-snapshot | 216075 | 703 | 1701 | 127,029 |
| Q1-windowed | 216075 | 86 | 1650 | 130,956 |
| Q2-continuous | 216075 | 61170 | 1289 | 167,631 |
| Q2-snapshot | 216075 | 140 | 1683 | 128,388 |
| Q2-windowed | 216075 | 50 | 1704 | 126,806 |
| Q3-continuous | 216075 | 216075 | 4618 | 46,790 |
| Q3-snapshot | 216075 | 97 | 2607 | 82,883 |
| Q3-windowed | 216075 | 50 | 4188 | 51,594 |
| Q4-continuous | 216075 | 62 | 9356 | 23,095 |
| Q4-snapshot | 216075 | 4685 | 10499 | 20,581 |
| Q4-windowed | 216075 | 98 | 9741 | 22,182 |
| Q5-continuous | 216075 | 60577 | 22889 | 9,440 |
| Q5-snapshot | 216075 | 34 | 2006 | 107,715 |
| Q5-windowed | 216075 | 6 | 2896 | 74,612 |
| Q6-continuous | 216075 | 216075 | 8708 | 24,814 |
| Q6-snapshot | 216075 | 698 | 7258 | 29,771 |
| Q6-windowed | 216075 | 203 | 6268 | 34,473 |
| Q7-continuous | 216075 | 5 | 4321 | 50,006 |
| Q7-snapshot | 216075 | 632 | 7487 | 28,860 |
| Q7-windowed | 216075 | 53 | 11625 | 18,587 |
| Q8-continuous | 216075 | 216075 | 4807 | 44,950 |
| Q8-snapshot | 216075 | 281 | 2825 | 76,487 |
| Q8-windowed | 216075 | 77 | 5946 | 36,340 |
| Q9-continuous | 216075 | 107870 | 4561 | 47,375 |
| Q9-snapshot | 216075 | 140 | 2281 | 94,729 |
| Q9-windowed | 216075 | 22 | 2384 | 90,636 |

The per-event MEOS-predicate cells that emit one row per input event
(Q3/Q8/Q9-continuous, 216,075 output rows through `edwithin_tgeo_geo` /
`eintersects_tgeo_geo`) sustain 45,000–47,000 ev/s. In the cross-platform
streaming catalog these cells run below MobilityFlink's in-JVM mini-cluster on
the same corpus, because Kafka Streams routes every record through the broker;
the per-cell shape matches across both engines. Q5-continuous is the O(V²)
all-pairs-meeting outlier (9,440 ev/s). Non-spatial cells (Q1/Q2) reach
110,000–168,000 ev/s. The `TopologyTestDriver` correctness harness
([`BerlinMODBenchmark`](../src/main/java/berlinmod/BerlinMODBenchmark.java))
reports output cardinality and correctness, not throughput: its per-event
bookkeeping dominates wall-clock, so a no-predicate cell already runs far below a
real broker's rate.
