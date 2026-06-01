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

import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * Local end-to-end test driver for the BerlinMOD-9 Kafka-Streams topology.
 *
 * <p>Runs the full {@link BerlinMODTopology} in-process via
 * {@link TopologyTestDriver}, pipes the shared 21-event synthetic corpus
 * plus a sentinel event at t=15001 to advance stream-time past the third
 * snapshot tick (t=15000), and reads every per-{@code Q<N>}-form output
 * topic with its appropriate deserializer.
 */
public class BerlinMODQ1LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ1LocalTest.class);
    private static final long T0 = 1_735_711_200_000L;
    // Two sentinels because Kafka Streams' STREAM_TIME punctuator coalesces
    // a multi-interval stream-time jump into a single fire — to get both
    // snapshot tick 15000 and tick 20000 we advance in two steps. The second
    // sentinel also closes the [10000, 20000) windowed cycle.
    private static final long SENTINEL_T1 = T0 + 15_001L;
    private static final long SENTINEL_T2 = T0 + 20_001L;

    public static void main(String[] args) {
        LOG.info("BerlinMODQ1LocalTest starting (TopologyTestDriver, all continuous + snapshot forms)");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "berlinmod-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

        BerlinMODTripSerde tripSerde = new BerlinMODTripSerde();

        try (TopologyTestDriver driver = new TopologyTestDriver(BerlinMODTopology.build(), props)) {

            TestInputTopic<Integer, BerlinMODTrip> input =
                    driver.createInputTopic(BerlinMODTopology.INPUT_TOPIC,
                                            new IntegerSerializer(),
                                            tripSerde.serializer());

            for (BerlinMODTrip trip : buildEvents()) {
                input.pipeInput(trip.getVehicleId(), trip,
                                Instant.ofEpochMilli(trip.getTimestamp()));
            }
            // Sentinel events (vehicleId == -1, ignored by all processors) to advance
            // stream-time across snapshot/windowed punctuator boundaries.
            input.pipeInput(-1, new BerlinMODTrip(-1, SENTINEL_T1, 0.0, 0.0),
                            Instant.ofEpochMilli(SENTINEL_T1));
            input.pipeInput(-1, new BerlinMODTrip(-1, SENTINEL_T2, 0.0, 0.0),
                            Instant.ofEpochMilli(SENTINEL_T2));

            // ---- continuous outputs ----
            readAndPrint(driver, BerlinMODTopology.Q1_CONTINUOUS_OUTPUT,
                         "Q1-continuous", Serdes.Integer().deserializer(), new LongDeserializer(),
                         (k, v) -> "(" + k + "," + v + ")");

            readAndPrintTrips(driver, BerlinMODTopology.Q2_CONTINUOUS_OUTPUT,
                              "Q2-continuous", tripSerde);

            readAndPrint(driver, BerlinMODTopology.Q3_CONTINUOUS_OUTPUT,
                         "Q3-continuous", Serdes.Integer().deserializer(), new BooleanDeserializer(),
                         (k, v) -> "(" + k + "," + v + ")");

            readAndPrint(driver, BerlinMODTopology.Q4_CONTINUOUS_OUTPUT,
                         "Q4-continuous", Serdes.Integer().deserializer(), new LongDeserializer(),
                         (k, v) -> "(" + k + "," + v + ")");

            readAndPrint(driver, BerlinMODTopology.Q5_CONTINUOUS_OUTPUT,
                         "Q5-continuous", new StringDeserializer(), new DoubleDeserializer(),
                         (k, v) -> k + " distance=" + v);

            readAndPrint(driver, BerlinMODTopology.Q6_CONTINUOUS_OUTPUT,
                         "Q6-continuous", Serdes.Integer().deserializer(), new DoubleDeserializer(),
                         (k, v) -> "v=" + k + " total=" + v);

            readAndPrint(driver, BerlinMODTopology.Q7_CONTINUOUS_OUTPUT,
                         "Q7-continuous", Serdes.Integer().deserializer(), new LongDeserializer(),
                         (k, v) -> "poi=" + k + " firstAt=" + v);

            readAndPrint(driver, BerlinMODTopology.Q8_CONTINUOUS_OUTPUT,
                         "Q8-continuous", Serdes.Integer().deserializer(), new BooleanDeserializer(),
                         (k, v) -> "(" + k + "," + v + ")");

            readAndPrint(driver, BerlinMODTopology.Q9_CONTINUOUS_OUTPUT,
                         "Q9-continuous", new LongDeserializer(), new DoubleDeserializer(),
                         (k, v) -> "t=" + k + " distance=" + v);

            // ---- windowed outputs ----
            readAndPrint(driver, BerlinMODTopology.Q1_WINDOWED_OUTPUT,
                         "Q1-windowed", new LongDeserializer(), new LongDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") count=" + v);

            readAndPrint(driver, BerlinMODTopology.Q3_WINDOWED_OUTPUT,
                         "Q3-windowed", new LongDeserializer(), new LongDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") count=" + v);

            readAndPrint(driver, BerlinMODTopology.Q8_WINDOWED_OUTPUT,
                         "Q8-windowed", new LongDeserializer(), new LongDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") count=" + v);

            readAndPrint(driver, BerlinMODTopology.Q2_WINDOWED_OUTPUT,
                         "Q2-windowed", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") " + v);

            readAndPrint(driver, BerlinMODTopology.Q4_WINDOWED_OUTPUT,
                         "Q4-windowed", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") " + v);

            readAndPrint(driver, BerlinMODTopology.Q5_WINDOWED_OUTPUT,
                         "Q5-windowed", new StringDeserializer(), new DoubleDeserializer(),
                         (k, v) -> k + " distance=" + v);

            readAndPrint(driver, BerlinMODTopology.Q6_WINDOWED_OUTPUT,
                         "Q6-windowed", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") " + v);

            readAndPrint(driver, BerlinMODTopology.Q7_WINDOWED_OUTPUT,
                         "Q7-windowed", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") " + v);

            readAndPrint(driver, BerlinMODTopology.Q9_WINDOWED_OUTPUT,
                         "Q9-windowed", new LongDeserializer(), new DoubleDeserializer(),
                         (k, v) -> "win[" + k + ", " + (k + BerlinMODTopology.WINDOW_SIZE_MILLIS) + ") distance=" + v);

            // ---- snapshot outputs ----
            readAndPrint(driver, BerlinMODTopology.Q1_SNAPSHOT_OUTPUT,
                         "Q1-snapshot", new LongDeserializer(), new IntegerDeserializer(),
                         (k, v) -> "(" + k + "," + v + ")");

            readAndPrint(driver, BerlinMODTopology.Q2_SNAPSHOT_OUTPUT,
                         "Q2-snapshot", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "T=" + k + " " + v);

            readAndPrint(driver, BerlinMODTopology.Q3_SNAPSHOT_OUTPUT,
                         "Q3-snapshot", new LongDeserializer(), new IntegerDeserializer(),
                         (k, v) -> "(" + k + "," + v + ")");

            readAndPrint(driver, BerlinMODTopology.Q4_SNAPSHOT_OUTPUT,
                         "Q4-snapshot", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "T=" + k + " " + v);

            readAndPrint(driver, BerlinMODTopology.Q5_SNAPSHOT_OUTPUT,
                         "Q5-snapshot", new StringDeserializer(), new DoubleDeserializer(),
                         (k, v) -> k + " distance=" + v);

            readAndPrint(driver, BerlinMODTopology.Q6_SNAPSHOT_OUTPUT,
                         "Q6-snapshot", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "T=" + k + " " + v);

            readAndPrint(driver, BerlinMODTopology.Q7_SNAPSHOT_OUTPUT,
                         "Q7-snapshot", new LongDeserializer(), new StringDeserializer(),
                         (k, v) -> "T=" + k + " " + v);

            readAndPrint(driver, BerlinMODTopology.Q8_SNAPSHOT_OUTPUT,
                         "Q8-snapshot", new LongDeserializer(), new IntegerDeserializer(),
                         (k, v) -> "(" + k + "," + v + ")");

            readAndPrint(driver, BerlinMODTopology.Q9_SNAPSHOT_OUTPUT,
                         "Q9-snapshot", new LongDeserializer(), new DoubleDeserializer(),
                         (k, v) -> "T=" + k + " distance=" + v);
        }
        LOG.info("BerlinMODQ1LocalTest done");
    }

    @FunctionalInterface
    private interface Fmt<K, V> { String render(K k, V v); }

    private static <K, V> void readAndPrint(
            TopologyTestDriver driver, String topic, String tag,
            org.apache.kafka.common.serialization.Deserializer<K> kd,
            org.apache.kafka.common.serialization.Deserializer<V> vd,
            Fmt<K, V> fmt) {
        TestOutputTopic<K, V> out = driver.createOutputTopic(topic, kd, vd);
        List<KeyValue<K, V>> records = out.readKeyValuesToList();
        System.out.println("=== " + tag + " output (" + records.size() + " lines) ===");
        for (KeyValue<K, V> kv : records) {
            System.out.println(tag + "> " + fmt.render(kv.key, kv.value));
        }
    }

    private static void readAndPrintTrips(
            TopologyTestDriver driver, String topic, String tag, BerlinMODTripSerde tripSerde) {
        TestOutputTopic<Integer, BerlinMODTrip> out =
                driver.createOutputTopic(topic, Serdes.Integer().deserializer(), tripSerde.deserializer());
        List<KeyValue<Integer, BerlinMODTrip>> records = out.readKeyValuesToList();
        System.out.println("=== " + tag + " output (" + records.size() + " lines) ===");
        for (KeyValue<Integer, BerlinMODTrip> kv : records) {
            System.out.println(String.format("%s> v=%d t=%d (%.4f,%.4f)",
                    tag, kv.value.getVehicleId(), kv.value.getTimestamp(),
                    kv.value.getLon(), kv.value.getLat()));
        }
    }

    private static List<BerlinMODTrip> buildEvents() {
        List<BerlinMODTrip> events = new ArrayList<>();
        for (int i = 0; i <= 12; i += 2) {
            events.add(new BerlinMODTrip(100, T0 + i * 1000L, 4.3517, 50.8503));
        }
        for (int i = 1; i <= 13; i += 2) {
            events.add(new BerlinMODTrip(200, T0 + i * 1000L, 4.3060, 50.8270));
        }
        for (int i = 0; i <= 12; i += 2) {
            events.add(new BerlinMODTrip(300, T0 + i * 1000L, 4.2000, 50.7500));
        }
        // Kafka-Streams STREAM_TIME punctuators fire on stream-time advance with
        // state-at-fire-moment. Sort by event-time so all vehicles' early events
        // are seen before the first tick, matching a real Kafka source's
        // monotonic event-time delivery.
        events.sort(Comparator.comparingLong(BerlinMODTrip::getTimestamp));
        return events;
    }
}
