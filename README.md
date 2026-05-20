
MobilityKafka
===============

An open-source geospatial trajectory data streaming platform based on Apache [Kafka](https://kafka.apache.org/).

<img src="doc/images/mobilitydb-logo.svg" width="200" alt="MobilityDB Logo" />

MobilityKafka explores the advantages of [MobilityDB](https://github.com/MobilityDB/MobilityDB) datatypes and functions in the Kafka environment, using the [JMEOS](https://github.com/MobilityDB/JMEOS) library as middleware.

The MobilityDB project is developed by the Computer & Decision Engineering Department of the [Université libre de Bruxelles](https://www.ulb.be/) (ULB) under the direction of [Prof. Esteban Zimányi](http://cs.ulb.ac.be/members/esteban/). ULB is an OGC Associate Member and member of the OGC Moving Feature Standard Working Group ([MF-SWG](https://www.ogc.org/projects/groups/movfeatswg)).

<img src="doc/images/OGC_Associate_Member_3DR.png" width="100" alt="OGC Associate Member Logo" />

More information about MobilityDB, including publications, presentations, etc., can be found in the MobilityDB [website](https://mobilitydb.com).


# BerlinMOD-9 × 3 streaming forms — the parity matrix on Kafka Streams

The streaming-side parity matrix runs all nine BerlinMOD reference queries (Q1..Q9) in three streaming forms each on this runtime: **continuous** (always-on, per-event emission), **windowed** (tumbling 10-second aggregation), and **snapshot** (5-second tick — the parity-oracle form whose output at watermark T equals the batch BerlinMOD-Q result on data up to T).

| Q | Topic | Continuous | Windowed | Snapshot |
|---|---|---|---|---|
| Q1 | "which vehicles have appeared in the stream?" | ✓ | ✓ | ✓ |
| Q2 | "where is vehicle X at time T?" | ✓ | ✓ | ✓ |
| Q3 | "vehicles within d of P at time T?" | ✓ | ✓ | ✓ |
| Q4 | "vehicles entered region R, and when?" | ✓ | ✓ | ✓ |
| Q5 | "pairs of vehicles meeting near P" | ✓ | ✓ | ✓ |
| Q6 | "cumulative distance per vehicle" | ✓ | ✓ | ✓ |
| Q7 | "first passage of vehicles through POIs" | ✓ | ✓ | ✓ |
| Q8 | "vehicles close to a road segment" | ✓ | ✓ | ✓ |
| Q9 | "distance between vehicles X and Y at time T" | ✓ | ✓ | ✓ |

**27 / 27 cells** = the full MobilityKafka parity-matrix row. Each cell has a dedicated `Q<N>{Continuous,Windowed,Snapshot}Processor` class in [`kafka-streams-app/src/main/java/berlinmod/`](kafka-streams-app/src/main/java/berlinmod/) and is locally verified via [`BerlinMODQ1LocalTest`](kafka-streams-app/src/main/java/berlinmod/BerlinMODQ1LocalTest.java) running on the Kafka-Streams `TopologyTestDriver` (no real broker required).

## Module structure

`kafka-streams-app/` is a Maven project (Java 21, Kafka Streams 3.6.0) holding:

- 27 per-cell `Q<N>{Continuous,Windowed,Snapshot}Processor` classes
- `BerlinMODTopology` — unified topology fanning input topic `berlinmod` to per-Q-form output topics
- `BerlinMODTrip` + `BerlinMODTripSerde` — shared data class + JSON Serde (byte-shape equivalent to MobilityFlink's `BerlinMODTrip`)
- `Haversine` + `SegmentDistance` + `PointOfInterest` — pure-Java geometry utilities used by the spatial-predicate cells
- `BerlinMODQ1LocalTest` — TopologyTestDriver-based local end-to-end driver

The streaming snapshot form converges to the batch BerlinMOD result on the same scale-factor corpus, anchored against the cross-platform outputs in [MobilityDB-BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD).

Spatial predicates today use pure-Java great-circle (`Haversine`) and planar segment-distance (`SegmentDistance`) utilities; each call site is marked `TODO(meos)` for JMEOS-bridge migration after [JMEOS#15](https://github.com/MobilityDB/JMEOS/pull/15) (the MEOS 1.4 regen) settles.

## Build and run

```
cd kafka-streams-app
mvn -q clean package -DskipTests
java --add-opens java.base/java.lang=ALL-UNNAMED \
     --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp target/mobility-kafka-streams-1.0-SNAPSHOT.jar \
     berlinmod.BerlinMODQ1LocalTest
```

The driver pipes a 21-event sorted-event-time corpus plus two sentinel records at `t = T0+15001` and `t = T0+20001` (to step the STREAM_TIME punctuator through the desired tick boundaries) and reads every per-Q-form output topic with the appropriate deserializer. Expected per-Q-form counts are in the PR body for the open scaffold PR.

## Sibling parity work in the ecosystem

- [MobilityFlink#3](https://github.com/MobilityDB/MobilityFlink/pull/3) — the same 27-cell row on Flink
- [MobilityNebula#15](https://github.com/MobilityDB/MobilityNebula/pull/15) — 15 of 27 cells on NebulaStream (Q1, Q2, Q3, Q4, Q7-via-POI-fanout)
- [MobilityDB-BerlinMOD#29](https://github.com/MobilityDB/MobilityDB-BerlinMOD/pull/29) — the batch BerlinMOD-9 cross-platform timings (the snapshot form's gold-answer source)
- [MobilityDB/.github#10](https://github.com/MobilityDB/.github/pull/10) — the ecosystem-profile description of the stream-layers tier

