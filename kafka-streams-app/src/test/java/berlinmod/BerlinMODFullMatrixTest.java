package berlinmod;

import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end regression test for the full BerlinMOD-9 × 3-form Kafka-Streams
 * parity matrix (all 27 cells, not just Q1).
 *
 * <p>Runs the whole {@link BerlinMODTopology} in-process via
 * {@link TopologyTestDriver}, pipes a slice of the canonical BerlinMOD corpus plus two
 * sentinel events that advance stream-time past the snapshot/windowed
 * punctuator boundaries, and asserts the per-{@code Q<N>}-form output-record
 * count for every continuous, windowed and snapshot topic.
 */
class BerlinMODFullMatrixTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODFullMatrixTest.class);

    @Test
    void fullMatrix() {
        LOG.info("BerlinMODFullMatrixTest starting (TopologyTestDriver, all 27 cells)");

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

            List<BerlinMODTrip> corpus = loadCanonicalSample();
            for (BerlinMODTrip trip : corpus) {
                input.pipeInput(trip.getVehicleId(), trip,
                                Instant.ofEpochMilli(trip.getTimestamp()));
            }
            // Sentinel events (vehicleId == -1, ignored by all processors) to advance
            // stream-time past the final snapshot/windowed punctuator boundaries after
            // the last canonical instant. Two steps because the STREAM_TIME punctuator
            // coalesces a multi-interval jump into a single fire.
            long lastTs = corpus.get(corpus.size() - 1).getTimestamp();
            for (long s : new long[]{lastTs + 6_000L, lastTs + 12_000L}) {
                input.pipeInput(-1, new BerlinMODTrip(-1, s, 0.0, 0.0), Instant.ofEpochMilli(s));
            }

            // Q5 ("pairs of vehicles meeting near P") is the one cell whose count
            // differs between the pure-Java Haversine approximation (scaffold) and
            // the MEOS geodetic engine (integration): the MEOS pair-meeting path
            // emits none under these params while the planar approximation is more
            // permissive. The other 26 cells are engine-invariant. TODO(meos):
            // reconcile the Q5 pair-meeting semantics across the two engines.
            boolean meos = Boolean.getBoolean("meos.enabled");

            // ---- continuous outputs ----
            assertCount(driver, BerlinMODTopology.Q1_CONTINUOUS_OUTPUT, "Q1-continuous", 5,
                        Serdes.Integer().deserializer(), new LongDeserializer());
            assertCount(driver, BerlinMODTopology.Q2_CONTINUOUS_OUTPUT, "Q2-continuous", 40,
                        Serdes.Integer().deserializer(), tripSerde.deserializer());
            assertCount(driver, BerlinMODTopology.Q3_CONTINUOUS_OUTPUT, "Q3-continuous", 200,
                        Serdes.Integer().deserializer(), new BooleanDeserializer());
            assertCount(driver, BerlinMODTopology.Q4_CONTINUOUS_OUTPUT, "Q4-continuous", 2,
                        Serdes.Integer().deserializer(), new LongDeserializer());
            assertCount(driver, BerlinMODTopology.Q5_CONTINUOUS_OUTPUT, "Q5-continuous", meos ? 0 : 199,
                        new StringDeserializer(), new DoubleDeserializer());
            assertCount(driver, BerlinMODTopology.Q6_CONTINUOUS_OUTPUT, "Q6-continuous", 200,
                        Serdes.Integer().deserializer(), new DoubleDeserializer());
            assertCount(driver, BerlinMODTopology.Q7_CONTINUOUS_OUTPUT, "Q7-continuous", 3,
                        Serdes.Integer().deserializer(), new LongDeserializer());
            assertCount(driver, BerlinMODTopology.Q8_CONTINUOUS_OUTPUT, "Q8-continuous", 200,
                        Serdes.Integer().deserializer(), new BooleanDeserializer());
            assertCount(driver, BerlinMODTopology.Q9_CONTINUOUS_OUTPUT, "Q9-continuous", 79,
                        new LongDeserializer(), new DoubleDeserializer());

            // ---- windowed outputs ----
            assertCount(driver, BerlinMODTopology.Q1_WINDOWED_OUTPUT, "Q1-windowed", 14,
                        new LongDeserializer(), new LongDeserializer());
            assertCount(driver, BerlinMODTopology.Q2_WINDOWED_OUTPUT, "Q2-windowed", 7,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q3_WINDOWED_OUTPUT, "Q3-windowed", 12,
                        new LongDeserializer(), new LongDeserializer());
            assertCount(driver, BerlinMODTopology.Q4_WINDOWED_OUTPUT, "Q4-windowed", 21,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q5_WINDOWED_OUTPUT, "Q5-windowed", meos ? 0 : 7,
                        new StringDeserializer(), new DoubleDeserializer());
            assertCount(driver, BerlinMODTopology.Q6_WINDOWED_OUTPUT, "Q6-windowed", 46,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q7_WINDOWED_OUTPUT, "Q7-windowed", 24,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q8_WINDOWED_OUTPUT, "Q8-windowed", 11,
                        new LongDeserializer(), new LongDeserializer());
            assertCount(driver, BerlinMODTopology.Q9_WINDOWED_OUTPUT, "Q9-windowed", 7,
                        new LongDeserializer(), new DoubleDeserializer());

            // ---- snapshot outputs ----
            assertCount(driver, BerlinMODTopology.Q1_SNAPSHOT_OUTPUT, "Q1-snapshot", 141,
                        new LongDeserializer(), new IntegerDeserializer());
            assertCount(driver, BerlinMODTopology.Q2_SNAPSHOT_OUTPUT, "Q2-snapshot", 28,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q3_SNAPSHOT_OUTPUT, "Q3-snapshot", 29,
                        new LongDeserializer(), new IntegerDeserializer());
            assertCount(driver, BerlinMODTopology.Q4_SNAPSHOT_OUTPUT, "Q4-snapshot", 57,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q5_SNAPSHOT_OUTPUT, "Q5-snapshot", meos ? 0 : 28,
                        new StringDeserializer(), new DoubleDeserializer());
            assertCount(driver, BerlinMODTopology.Q6_SNAPSHOT_OUTPUT, "Q6-snapshot", 141,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q7_SNAPSHOT_OUTPUT, "Q7-snapshot", 84,
                        new LongDeserializer(), new StringDeserializer());
            assertCount(driver, BerlinMODTopology.Q8_SNAPSHOT_OUTPUT, "Q8-snapshot", 56,
                        new LongDeserializer(), new IntegerDeserializer());
            assertCount(driver, BerlinMODTopology.Q9_SNAPSHOT_OUTPUT, "Q9-snapshot", 28,
                        new LongDeserializer(), new DoubleDeserializer());
        }
        LOG.info("BerlinMODFullMatrixTest done — all 27 cells matched expected counts");
    }

    private static <K, V> void assertCount(
            TopologyTestDriver driver, String topic, String tag, int expected,
            Deserializer<K> kd, Deserializer<V> vd) {
        int actual = driver.createOutputTopic(topic, kd, vd).readKeyValuesToList().size();
        LOG.info("{} output: {} lines (expected {})", tag, actual, expected);
        LOG.info("{} output: {} lines (expected {})", tag, actual, expected);
        assertEquals(expected, actual, tag + " output-record count");
    }

    /**
     * Loads the canonical BerlinMOD sample (real instants for vehicles 1-5,
     * reprojected to WGS84 and time-aligned to a common base) from the test
     * resources. This is a slice of the single canonical BerlinMOD corpus
     * shared across the ecosystem — no invented coordinates.
     */
    private static List<BerlinMODTrip> loadCanonicalSample() {
        List<BerlinMODTrip> events = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                BerlinMODFullMatrixTest.class.getResourceAsStream("/berlinmod_sample.csv"),
                StandardCharsets.UTF_8))) {
            r.readLine(); // header: vehicleId,timestampMs,lon,lat
            for (String line; (line = r.readLine()) != null; ) {
                String[] f = line.split(",");
                events.add(new BerlinMODTrip(Integer.parseInt(f[0].trim()),
                        Long.parseLong(f[1].trim()),
                        Double.parseDouble(f[2].trim()),
                        Double.parseDouble(f[3].trim())));
            }
        } catch (Exception e) {
            throw new RuntimeException("loading /berlinmod_sample.csv", e);
        }
        // Sort by event-time so all vehicles' early instants are seen before the
        // first punctuator tick, matching a real Kafka source's monotonic delivery.
        events.sort(Comparator.comparingLong(BerlinMODTrip::getTimestamp));
        return events;
    }
}
