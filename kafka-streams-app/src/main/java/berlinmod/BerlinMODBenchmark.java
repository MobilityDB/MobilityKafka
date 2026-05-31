package berlinmod;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Per-cell throughput benchmark for the BerlinMOD-9 × 3-form streaming matrix on
 * Kafka Streams.
 *
 * <p>Each cell runs in isolation through its own single-cell topology
 * ({@link BerlinMODTopology#buildCell}) and {@link TopologyTestDriver} over the
 * corpus — the Kafka-Streams analog of MobilityFlink's per-cell jobs, so the
 * per-cell wall-clock and throughput are independent and comparable. The corpus
 * and the per-query parameters are corpus-derived via {@link BerlinMODCorpus};
 * the spatial predicates evaluate through MEOS (see {@link MEOSBridge}).
 *
 * <pre>
 *   java … berlinmod.BerlinMODBenchmark --csv &lt;berlinmod_instants.csv&gt; [--max N]
 *   java … berlinmod.BerlinMODBenchmark --vehicles 50 --events 600 [--only Q3-continuous]
 * </pre>
 */
public final class BerlinMODBenchmark {

    private BerlinMODBenchmark() { /* utility */ }

    public static void main(String[] args) throws Exception {
        String csv = null, only = null;
        int maxRows = 0, vehicles = 50, events = 600;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--csv": csv = args[++i]; break;
                case "--max": maxRows = Integer.parseInt(args[++i]); break;
                case "--vehicles": vehicles = Integer.parseInt(args[++i]); break;
                case "--events": events = Integer.parseInt(args[++i]); break;
                case "--only": only = args[++i]; break;
                default: break;
            }
        }
        List<BerlinMODTrip> corpus = csv != null
                ? BerlinMODCorpus.fromInstantsCsv(csv, maxRows)
                : BerlinMODCorpus.synthetic(vehicles, events);
        int n = corpus.size();
        long maxTs = corpus.stream().mapToLong(BerlinMODTrip::getTimestamp).max().orElse(0L);
        BerlinMODCorpus.Params p = BerlinMODCorpus.derive(corpus);
        System.out.printf("Corpus: %s, %d events; window=%ds tick=%dms%n",
                csv != null ? "real BerlinMOD instants" : "synthetic", n, p.windowSeconds, p.snapshotTickMillis);

        // (cellName, outputTopic) enumerated from the topology's *_OUTPUT constants.
        TreeMap<String, String> cells = new TreeMap<>();
        for (Field f : BerlinMODTopology.class.getFields()) {
            if (f.getName().endsWith("_OUTPUT") && f.getType() == String.class) {
                cells.put(cellName(f.getName()), (String) f.get(null));
            }
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "berlinmod-bench");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        BerlinMODTripSerde tripSerde = new BerlinMODTripSerde();

        List<String[]> rows = new ArrayList<>();
        for (var e : cells.entrySet()) {
            String cell = e.getKey(), topic = e.getValue();
            if (only != null && !cell.equals(only)) {
                continue;
            }
            long out;
            long t0 = System.nanoTime();
            try (TopologyTestDriver driver = new TopologyTestDriver(BerlinMODTopology.buildCell(p, cell), props)) {
                TestInputTopic<Integer, BerlinMODTrip> input = driver.createInputTopic(
                        BerlinMODTopology.INPUT_TOPIC, new IntegerSerializer(), tripSerde.serializer());
                for (BerlinMODTrip trip : corpus) {
                    input.pipeInput(trip.getVehicleId(), trip, Instant.ofEpochMilli(trip.getTimestamp()));
                }
                input.pipeInput(-1, new BerlinMODTrip(-1, maxTs + 3_600_000L, 0.0, 0.0),
                        Instant.ofEpochMilli(maxTs + 3_600_000L));
                input.pipeInput(-1, new BerlinMODTrip(-1, maxTs + 7_200_000L, 0.0, 0.0),
                        Instant.ofEpochMilli(maxTs + 7_200_000L));
                TestOutputTopic<byte[], byte[]> output = driver.createOutputTopic(
                        topic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                out = output.readRecordsToList().size();
            }
            long wallMs = (System.nanoTime() - t0) / 1_000_000L;
            double tput = wallMs > 0 ? n / (wallMs / 1000.0) : 0;
            rows.add(new String[]{cell, String.valueOf(n), String.valueOf(out),
                    String.valueOf(wallMs), String.format("%,.0f", tput)});
            System.out.printf("  %-14s out=%-8d %6d ms  %,.0f ev/s%n", cell, out, wallMs, tput);
        }

        System.out.println();
        System.out.println("| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |");
        System.out.println("|---|---:|---:|---:|---:|");
        for (String[] r : rows) {
            System.out.printf("| %s | %s | %s | %s | %s |%n", r[0], r[1], r[2], r[3], r[4]);
        }
    }

    /** Q3_CONTINUOUS_OUTPUT → Q3-continuous */
    private static String cellName(String field) {
        String s = field.substring(0, field.length() - "_OUTPUT".length());
        int us = s.indexOf('_');
        return s.substring(0, us) + "-" + s.substring(us + 1).toLowerCase();
    }
}
