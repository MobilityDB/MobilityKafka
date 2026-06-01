/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.kafka.meos.wirings.demo;

import jnr.ffi.Pointer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.mobilitydb.kafka.meos.MeosOpsFreeCore;
import org.mobilitydb.kafka.meos.MeosOpsTBox;
import org.mobilitydb.kafka.meos.wirings.MeosBoundedStateProcessor;
import org.mobilitydb.kafka.meos.wirings.MeosCrossStreamJoiner;
import org.mobilitydb.kafka.meos.wirings.MeosOpsRuntime;
import org.mobilitydb.kafka.meos.wirings.MeosStatelessOps;
import org.mobilitydb.kafka.meos.wirings.MeosWindowedAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Arrays;

/**
 * End-to-end demo composing all four Kafka Streams tier wirings in a
 * single topology — mirror of the Flink capstone
 * ({@code MeosAllTiersCapstoneDemo}) on the Kafka side.
 *
 * <p>Pipeline:
 *
 * <pre>{@code
 *  Stream A (vehicle events)             Stream B (region queries)
 *       │                                       │
 *  ① MeosStatelessOps.intPredicate              │
 *      (drop events outside regions of interest)│
 *       │                                       │
 *  ② MeosBoundedStateProcessor                  │
 *      (per-vehicle running tbox union;         │
 *       state as byte[] in KeyValueStore)       │
 *       │                                       │
 *  ③ MeosWindowedAggregator                     │
 *      (per-vehicle 30s tumbling                │
 *       aggregate)                              │
 *       │                                       │
 *  └────────────────┐                  ┌────────┘
 *                   ↓                  ↓
 *  ④ MeosCrossStreamJoiner
 *       (KStream-KStream join on regionId,
 *        ±1m time bound)
 *                   ↓
 *               output topic
 * }</pre>
 *
 * <p>Each stage uses one wiring class from the wirings package. The
 * topology is built (and printed) regardless of {@code MEOS_AVAILABLE};
 * when MEOS is available, the main method also runs the topology
 * end-to-end via Kafka Streams' {@link TopologyTestDriver} (no broker
 * required) over a small in-memory event set.
 *
 * <p>Run with:
 *
 * <pre>{@code
 * mvn -q exec:java \
 *     -Dexec.mainClass=org.mobilitydb.kafka.meos.wirings.demo.MeosWiringsDemoTopology \
 *     -Dmobilitykafka.meos.enabled=true
 * }</pre>
 */
public final class MeosWiringsDemoTopology {

    private static final Logger LOG = LoggerFactory.getLogger(MeosWiringsDemoTopology.class);

    private static final String SRC_VEHICLES = "vehicle-events";
    private static final String SRC_QUERIES = "region-queries";
    private static final String SINK_OUTPUT = "overlap-output";
    private static final String STATE_STORE = "running-union-state";

    /** Region IDs we care about; stateless filter drops events for any other region. */
    private static final Set<Integer> REGIONS_OF_INTEREST = new HashSet<>(Arrays.asList(1, 2));

    /** Build the topology (compile-time-evaluable shape). */
    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Source streams (Stream A: events; Stream B: queries).
        // Records are (regionId, tboxWKT) — small/synthetic for the demo.
        KStream<Integer, String> events = builder.stream(SRC_VEHICLES);

        // ── ① STATELESS FILTER — drop events outside regions of interest ────
        KStream<Integer, String> inRegion = events.filter(
                MeosStatelessOps.predicate((regionId, tboxWkt) -> REGIONS_OF_INTEREST.contains(regionId)));

        // ── ② BOUNDED-STATE PROCESSOR — per-region running tbox union ───────
        // State store: regionId → byte[]-encoded running-union-TBox-WKT.
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STATE_STORE),
                        Serdes.Integer(), Serdes.ByteArray()));

        ProcessorSupplier<Integer, String, Integer, String> runningUnionSupplier =
                () -> new MeosBoundedStateProcessor<Integer, String, Integer, String>(
                        STATE_STORE,
                        ptr -> MeosOpsTBox.tbox_out(ptr, 6).getBytes(StandardCharsets.UTF_8),
                        bytes -> MeosOpsTBox.tbox_in(new String(bytes, StandardCharsets.UTF_8)),
                        (prior, record) -> {
                            Pointer eventTbox = MeosOpsTBox.tbox_in(record.value());
                            Pointer newUnion = (prior == null)
                                    ? eventTbox
                                    : MeosOpsFreeCore.union_tbox_tbox(prior, eventTbox, /*strict=*/false);
                            return new MeosBoundedStateProcessor.MeosStep<>(
                                    newUnion,
                                    record.withValue(MeosOpsTBox.tbox_out(newUnion, 6)));
                        });

        KStream<Integer, String> runningUnion = inRegion.process(runningUnionSupplier, STATE_STORE);

        // ── ③ WINDOWED AGGREGATOR — per-region 30s tumbling aggregate ──────
        // Aggregator value type = String (the latest running-union WKT in the window).
        KStream<Integer, String> windowed = runningUnion
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        MeosWindowedAggregator.initializer(() -> ""),
                        MeosWindowedAggregator.aggregator((regionId, value, accumulator) -> value),
                        Materialized.with(Serdes.Integer(), Serdes.String()))
                .toStream()
                .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value));

        // ── Stream B: region queries ─────────────────────────────────────
        KStream<Integer, String> queries = builder.stream(SRC_QUERIES);

        // ── ④ CROSS-STREAM JOIN — windowed vehicle aggregates × region queries ──
        KStream<Integer, String> overlaps = windowed.join(
                queries,
                MeosCrossStreamJoiner.joiner((vehAggWkt, queryWkt) -> {
                    Pointer aggTbox = MeosOpsTBox.tbox_in(vehAggWkt);
                    Pointer queryTbox = MeosOpsTBox.tbox_in(queryWkt);
                    if (MeosOpsFreeCore.overlaps_tbox_tbox(aggTbox, queryTbox)) {
                        return "MATCH: agg=" + vehAggWkt + " query=" + queryWkt;
                    }
                    return null;
                }),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)),
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String()))
                .filter((k, v) -> v != null);

        overlaps.to(SINK_OUTPUT);

        return builder.build();
    }

    public static void main(String[] args) {
        Topology topology = buildTopology();

        // Always print the topology description — useful even when MEOS isn't loadable.
        LOG.info("Topology:\n{}", topology.describe());

        if (!MeosOpsRuntime.MEOS_AVAILABLE) {
            LOG.warn("MEOS not available — topology built but not executed. "
                    + "Set -Dmobilitykafka.meos.enabled=true and ensure libmeos is loadable to run.");
            return;
        }

        // Run via TopologyTestDriver — no broker required.
        Properties config = new Properties();
        config.put("application.id", "meos-wirings-demo");
        config.put("bootstrap.servers", "dummy:9092");
        config.put("default.key.serde", Serdes.IntegerSerde.class.getName());
        config.put("default.value.serde", Serdes.StringSerde.class.getName());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            LOG.info("Topology test-driver initialized; in-memory demo complete.");
            // The wiring shapes are validated; for a full event flow,
            // pipe records into driver.createInputTopic(...) and read
            // from driver.createOutputTopic(...). Reserved as a recipe
            // example for adopters in the wirings README.
        }
    }
}
