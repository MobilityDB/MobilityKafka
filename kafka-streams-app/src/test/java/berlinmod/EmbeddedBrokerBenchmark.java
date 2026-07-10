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

package berlinmod;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Runtime throughput benchmark for the BerlinMOD-9 × 3-form streaming matrix on
 * Kafka Streams against a real, in-process Kafka broker.
 *
 * <p>This is the Flink-comparable counterpart of {@link BerlinMODBenchmark}: the
 * corpus is produced once into the {@link BerlinMODTopology#INPUT_TOPIC} topic of
 * an {@link EmbeddedKafkaCluster} (a genuine {@code KafkaServer} reachable over
 * the loopback network), and each cell runs as its own {@link KafkaStreams}
 * application — the analog of MobilityFlink's per-cell jobs — consuming from the
 * shared input topic and writing to the cell's output topic. The spatial
 * predicates evaluate through MEOS (see {@link MEOSBridge}); the corpus and the
 * per-query parameters are corpus-derived via {@link BerlinMODCorpus}, exactly as
 * in MobilityFlink.
 *
 * <p>Throughput is the corpus size divided by the wall-clock from streams start
 * until the application has consumed the whole input topic (its committed offset
 * reaches the input end offset) and its output has gone idle. Each consumed
 * record runs the cell's MEOS predicate, so this is the steady-state per-event
 * processing rate, directly comparable to the MobilityFlink figures. The trailing
 * idle settling time is excluded from the wall.
 *
 * <p>Run from {@code kafka-streams-app/} with an extended libmeos on the loader
 * path and the test classpath (it carries the embedded broker):
 *
 * <pre>
 *   CP=$(mvn -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout | tail -1)
 *   LD_LIBRARY_PATH=&lt;libmeos-dir&gt; java -cp target/classes:target/test-classes:jar/JMEOS.jar:$CP \
 *     berlinmod.EmbeddedBrokerBenchmark --csv &lt;berlinmod_instants.csv&gt; [--max N] [--only Q3-continuous]
 * </pre>
 */
public final class EmbeddedBrokerBenchmark {

    private EmbeddedBrokerBenchmark() { /* utility */ }

    /** Settle window with no new output before a cell is declared finished. */
    private static final long QUIET_MS = 2_000L;
    /** Hard per-cell ceiling so a stuck cell cannot hang the whole run. */
    private static final long CELL_TIMEOUT_MS = 300_000L;

    public static void main(String[] args) throws Exception {
        String csv = null, only = null;
        int maxRows = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--csv": csv = args[++i]; break;
                case "--max": maxRows = Integer.parseInt(args[++i]); break;
                case "--only": only = args[++i]; break;
                default: break;
            }
        }

        if (csv == null) {
            System.err.println("--csv <berlinmod_instants.csv> is required: the benchmark runs "
                    + "on the canonical BerlinMOD corpus only.");
            System.exit(2);
        }
        List<BerlinMODTrip> corpus = BerlinMODCorpus.fromInstantsCsv(csv, maxRows);
        int n = corpus.size();
        long maxTs = corpus.stream().mapToLong(BerlinMODTrip::getTimestamp).max().orElse(0L);
        BerlinMODCorpus.Params p = BerlinMODCorpus.derive(corpus);
        System.out.printf("Corpus: %s, %d events; window=%ds tick=%dms%n",
                csv != null ? "real BerlinMOD instants" : "synthetic", n, p.windowSeconds, p.snapshotTickMillis);

        // (cellName -> outputTopic) enumerated from the topology's *_OUTPUT constants.
        TreeMap<String, String> cells = new TreeMap<>();
        for (Field f : BerlinMODTopology.class.getFields()) {
            if (f.getName().endsWith("_OUTPUT") && f.getType() == String.class) {
                cells.put(cellName(f.getName()), (String) f.get(null));
            }
        }

        // Each cell runs against its OWN fresh embedded broker — the true analog of
        // MobilityFlink's independent per-cell jobs. A single broker shared across all
        // 27 cells destabilises after a handful of KafkaStreams lifecycles on one node;
        // a fresh broker per cell keeps every measurement isolated and reproducible.
        List<String[]> rows = new ArrayList<>();
        for (var e : cells.entrySet()) {
            String cell = e.getKey(), topic = e.getValue();
            if (only != null && !cell.equals(only)) {
                continue;
            }
            EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(1, brokerProps());
            cluster.start();
            try {
                cluster.createTopic(BerlinMODTopology.INPUT_TOPIC, 1, 1);
                long inputEnd = produceCorpus(cluster.bootstrapServers(), corpus, maxTs);
                cluster.createTopic(topic, 1, 1);
                String[] r = runCell(cluster.bootstrapServers(), cell, topic, p, n, inputEnd);
                rows.add(r);
                System.out.printf("  %-14s out=%-8s %7s ms  %s ev/s%n", cell, r[2], r[3], r[4]);
            } finally {
                cluster.stop();
            }
        }

        System.out.println();
        System.out.println("| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |");
        System.out.println("|---|---:|---:|---:|---:|");
        for (String[] r : rows) {
            System.out.printf("| %s | %s | %s | %s | %s |%n", r[0], r[1], r[2], r[3], r[4]);
        }
    }

    /** Single-broker, single-ISR config for the embedded cluster. */
    private static Properties brokerProps() {
        Properties bp = new Properties();
        bp.put("auto.create.topics.enable", "true");
        bp.put("offsets.topic.replication.factor", "1");
        bp.put("transaction.state.log.replication.factor", "1");
        bp.put("transaction.state.log.min.isr", "1");
        bp.put("group.initial.rebalance.delay.ms", "0");
        return bp;
    }

    /** Produce the corpus plus two future-timestamped flush sentinels; returns the input end offset. */
    private static long produceCorpus(String bootstrap, List<BerlinMODTrip> corpus, long maxTs) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        pp.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        BerlinMODTripSerde tripSerde = new BerlinMODTripSerde();
        long count = 0;
        try (KafkaProducer<Integer, byte[]> producer =
                     new KafkaProducer<>(pp, new IntegerSerializer(), new ByteArraySerializer())) {
            for (BerlinMODTrip trip : corpus) {
                byte[] v = tripSerde.serializer().serialize(BerlinMODTopology.INPUT_TOPIC, trip);
                producer.send(new ProducerRecord<>(BerlinMODTopology.INPUT_TOPIC, null,
                        trip.getTimestamp(), trip.getVehicleId(), v));
                count++;
            }
            // Two flush sentinels advance event time so windowed/snapshot cells close.
            for (long delta : new long[]{3_600_000L, 7_200_000L}) {
                BerlinMODTrip s = new BerlinMODTrip(-1, maxTs + delta, 0.0, 0.0);
                byte[] v = tripSerde.serializer().serialize(BerlinMODTopology.INPUT_TOPIC, s);
                producer.send(new ProducerRecord<>(BerlinMODTopology.INPUT_TOPIC, null,
                        maxTs + delta, -1, v));
                count++;
            }
            producer.flush();
        }
        return count;
    }

    /** Run one cell as a real KafkaStreams app; returns {cell, eventsIn, outputRows, wallMs, throughput}. */
    private static String[] runCell(String bootstrap, String cell, String outputTopic,
                                    BerlinMODCorpus.Params p, int n, long inputEnd) throws Exception {
        String appId = "berlinmod-bench-" + cell.toLowerCase().replace('-', '_');
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("bench-" + appId).toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);

        long outputRows;
        long wallMs;
        long consumedAtEnd;
        try (Admin admin = Admin.create(adminProps);
             KafkaConsumer<byte[], byte[]> outConsumer = outputConsumer(bootstrap, outputTopic)) {

            KafkaStreams streams = new KafkaStreams(BerlinMODTopology.buildCell(p, cell), props);
            long t0 = System.nanoTime();
            streams.start();

            long outCount = 0;
            long tInputDrained = -1;
            long tLastOutput = t0;
            long deadline = System.currentTimeMillis() + CELL_TIMEOUT_MS;
            // Progress is read straight from the running app's own consumer metric
            // (records-consumed-total for the input topic), so completion detection
            // is independent of the group coordinator — which becomes unreliable on a
            // single broker after many app lifecycles.
            while (true) {
                ConsumerRecords<byte[], byte[]> recs = outConsumer.poll(Duration.ofMillis(100));
                if (!recs.isEmpty()) {
                    outCount += recs.count();
                    tLastOutput = System.nanoTime();
                }
                if (tInputDrained < 0 && inputConsumed(streams) >= inputEnd) {
                    tInputDrained = System.nanoTime();
                }
                long progress = outCount > 0 ? Math.max(tInputDrained, tLastOutput) : tInputDrained;
                boolean idle = tInputDrained > 0 && (System.nanoTime() - progress) / 1_000_000L >= QUIET_MS;
                if (idle) {
                    wallMs = (progress - t0) / 1_000_000L;
                    break;
                }
                if (System.currentTimeMillis() > deadline) {
                    wallMs = (System.nanoTime() - t0) / 1_000_000L;
                    System.out.printf("  [%s] timed out after %d ms (consumed=%d/%d)%n",
                            cell, wallMs, inputConsumed(streams), inputEnd);
                    break;
                }
            }
            outputRows = outCount;
            consumedAtEnd = inputConsumed(streams);
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
            cleanup(admin, appId, outputTopic);
        }

        // Throughput uses the events actually consumed (the input topic high
        // watermark, == inputEnd once drained), divided by the measured wall.
        long events = Math.min(consumedAtEnd, inputEnd);
        double tput = wallMs > 0 ? events / (wallMs / 1000.0) : 0;
        return new String[]{cell, String.valueOf(n), String.valueOf(outputRows),
                String.valueOf(wallMs), String.format("%,.0f", tput)};
    }

    /** Records the running app's consumer has read from the input topic, via its own metrics. */
    private static long inputConsumed(KafkaStreams streams) {
        double sum = 0;
        for (Metric m : streams.metrics().values()) {
            MetricName name = m.metricName();
            if ("records-consumed-total".equals(name.name())
                    && BerlinMODTopology.INPUT_TOPIC.equals(name.tags().get("topic"))) {
                Object v = m.metricValue();
                if (v instanceof Number) {
                    sum += ((Number) v).doubleValue();
                }
            }
        }
        return (long) sum;
    }

    /** Output consumer with an explicit partition assignment — no group, no rebalance, no coordinator load. */
    private static KafkaConsumer<byte[], byte[]> outputConsumer(String bootstrap, String outputTopic) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<byte[], byte[]> c =
                new KafkaConsumer<>(cp, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        TopicPartition tp = new TopicPartition(outputTopic, 0);
        c.assign(Collections.singletonList(tp));
        c.seekToBeginning(Collections.singletonList(tp));
        return c;
    }

    /** Delete the cell's consumer group and internal topics so the coordinator stays lean across cells. */
    private static void cleanup(Admin admin, String appId, String outputTopic) {
        try {
            admin.deleteConsumerGroups(Collections.singletonList(appId)).all().get();
        } catch (Exception ignored) {
            // group may already be gone
        }
        deleteInternalTopics(admin, appId);
    }

    private static void deleteInternalTopics(Admin admin, String appId) {
        try {
            List<String> internal = new ArrayList<>();
            for (String t : admin.listTopics().names().get()) {
                if (t.startsWith(appId + "-")) {
                    internal.add(t);
                }
            }
            if (!internal.isEmpty()) {
                admin.deleteTopics(internal).all().get();
            }
        } catch (Exception ignored) {
            // best-effort cleanup; the cluster is torn down at the end anyway
        }
    }

    /** Q3_CONTINUOUS_OUTPUT -> Q3-continuous */
    private static String cellName(String field) {
        String s = field.substring(0, field.length() - "_OUTPUT".length());
        int us = s.indexOf('_');
        return s.substring(0, us) + "-" + s.substring(us + 1).toLowerCase();
    }
}
