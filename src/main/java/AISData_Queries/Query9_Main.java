package AISData_Queries;

import functions.functions;
import functions.MeosErrorHandler;
import jnr.ffi.Pointer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Query9_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query9_Main.class);

    /** Number of nearest neighbours to retain per device: the {@code k} in knn_agg. */
    private static final int K = 3;

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query9_AIS");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            StreamsBuilder builder = new StreamsBuilder();

            TimestampExtractor myExtractor = new MyTimeStampExtractor();

            KStream<String, String> stream1 = builder.stream("query-input", Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(myExtractor));

            KGroupedStream<String, String> gps = stream1
                    .filter((key, value) -> value != null && !value.startsWith("t,"))
                    .map((k, v) -> new KeyValue<>("all", v)) //using an arbitrary constant key
                    .groupByKey(Grouped.as("gps1-group"));

            KStream<String, String> stream2 = builder.stream("query-input", Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(myExtractor));

            KGroupedStream<String, String> gps2 = stream2
                    .filter((key, value) -> value != null && !value.startsWith("t,"))
                    .map((k, v) -> new KeyValue<>("all", v)) //using the same key as earlier so that everything is in the same partition
                    .groupByKey(Grouped.as("gps2-group"));

            KStream<String, Object> result = gps
                    .cogroup((key, value, aggregate) -> aggregate + "GPS1:" + value + ";")
                    .cogroup(
                            gps2,
                            (key, value, aggregate) -> aggregate + "GPS2:" + value + ";"
                    )
                    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)))
                    .aggregate(
                            () -> ""
                    )
                    .toStream()
                    .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value));

            result.process(new Query9_Main.KnnCoGroupFunction(K))
                    .to("query-output");

            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.cleanUp();
            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            Thread.currentThread().join();

            logger.info("Done");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                logger.info("Finalizing MEOS library");
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    public static class KnnCoGroupFunction implements ProcessorSupplier<String, Object, String, String> {

        private final int k;

        public KnnCoGroupFunction(int k) {
            this.k = k;
        }

        @Override
        public Processor<String, Object, String, String> get() {
            return new Processor<String, Object, String, String>() {


                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query7_Main.ClosestPairsCoGroupFunction.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler;


                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);
                }

                @Override
                public void process(Record<String, Object> record) {
                    String[] rows = record.value().toString().split(";");

                    List<AISData> lefts  = new ArrayList<>();
                    List<AISData> rights = new ArrayList<>();

                    for (String row : rows) {
                        if(row.startsWith("GPS1:")) {
                            row = row.substring(row.indexOf("GPS1:") + 5);
                            lefts.add(AISData.fromCsv(row));
                        }else if(row.startsWith("GPS2:")) {
                            row = row.substring(row.indexOf("GPS2:") + 5);
                            rights.add(AISData.fromCsv(row));
                        }
                    }

                    if (lefts.isEmpty() || rights.isEmpty()) return;

                    // Same precalculation logic as in Query 7
                    List<Pointer> geoLefts  = new ArrayList<>(lefts.size());
                    for (AISData left : lefts) {
                        String ts = millisToTimestamp(left.getTimestamp());
                        Pointer tp  = functions.tgeogpoint_in(
                                String.format("POINT(%f %f)@%s", left.getLon(), left.getLat(), ts));
                        Pointer geo = (tp != null) ? functions.tgeo_end_value(tp) : null;
                        geoLefts.add(geo); // null if tgeogpoint_in or temporal_end_value failed
                    }

                    List<Pointer> geoRights = new ArrayList<>(rights.size());
                    for (AISData right : rights) {
                        String ts = millisToTimestamp(right.getTimestamp());
                        Pointer tp  = functions.tgeogpoint_in(
                                String.format("POINT(%f %f)@%s", right.getLon(), right.getLat(), ts));
                        Pointer geo = (tp != null) ? functions.tgeo_end_value(tp) : null;
                        geoRights.add(geo);
                    }

                    // Step 1: cross-product with mmsi1 != mmsi2, keeping min dist per (mmsi1, mmsi2).
                    //
                    // Unlike Query 7 (mmsi1 < mmsi2), here we keep BOTH (A,B) and (B,A) because
                    // A's kNN list and B's kNN list are computed independently.
                    // The deduplication map ensures we keep the minimum distance over all timestamp
                    // combinations for each directed pair.
                    Map<String, double[]> minDistMap = new HashMap<>();
                    // value: [mmsi1, mmsi2, dist, lon1, lat1, lon2, lat2]

                    for (int i = 0; i < lefts.size(); i++) {
                        AISData left    = lefts.get(i);
                        Pointer geoLeft = geoLefts.get(i);
                        if (geoLeft == null) continue;

                        for (int j = 0; j < rights.size(); j++) {
                            AISData right    = rights.get(j);
                            Pointer geoRight = geoRights.get(j);
                            if (geoRight == null) continue;

                            // Paper Line 2: device_id != device_id2
                            if (left.getMmsi() == right.getMmsi()) continue;

                            double dist = functions.geog_distance(geoLeft, geoRight);

                            // Key: directed pair "mmsi1→mmsi2"
                            String key = left.getMmsi() + "->" + right.getMmsi();
                            if (!minDistMap.containsKey(key) || dist < minDistMap.get(key)[2]) {
                                minDistMap.put(key, new double[]{
                                        left.getMmsi(), right.getMmsi(), dist,
                                        left.getLon(), left.getLat(),
                                        right.getLon(), right.getLat()});
                            }
                        }
                    }
                    if (minDistMap.isEmpty()) return;

                    // Step 2: groupBy device_id (paper Line 5):
                    // Build a per-mmsi1 list of (mmsi2, dist) entries.
                    Map<Integer, List<double[]>> byDevice = new HashMap<>();
                    for (double[] entry : minDistMap.values()) {
                        int mmsi1 = (int) entry[0];
                        byDevice.computeIfAbsent(mmsi1, x -> new ArrayList<>()).add(entry);
                    }

                    // Step 3: knn_agg(mindist, device_id2, k) (paper Line 6):
                    // For each device, sort by dist ascending and keep the k nearest neighbours.
                    for (Map.Entry<Integer, List<double[]>> deviceEntry : byDevice.entrySet()) {
                        int          mmsi1     = deviceEntry.getKey();
                        List<double[]> neighbours = deviceEntry.getValue();

                        neighbours.sort(Comparator.comparingDouble(e -> e[2]));

                        int emitCount = Math.min(k, neighbours.size());
                        StringBuilder sb = new StringBuilder();
                        sb.append(String.format("[KNN][Q9] MMSI=%-12d | k=%d/%d neighbours:%n",
                                mmsi1, emitCount, neighbours.size()));

                        for (int rank = 0; rank < emitCount; rank++) {
                            double[] nb = neighbours.get(rank);
                            sb.append(String.format(
                                    "           rank=%d | neighbour=%-12d"
                                            + " (lon=%9.5f lat=%8.5f) | mindist=%10.3f m%n",
                                    rank + 1, (long) nb[1], nb[5], nb[6], nb[2]));
                        }

                        String result = sb.toString().stripTrailing();
                        log.info(result);
                        context.forward(new Record<>(String.valueOf(mmsi1), result, record.timestamp()));
                    }

                }

                @Override
                public void close() {
                    Processor.super.close();
                    functions.meos_finalize();
                }

                private String millisToTimestamp(long millis) {
                    Instant instant = Instant.ofEpochMilli(millis);
                    OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
                    return dt.format(TIMESTAMP_FMT);
                }
            };
        }
    }
}
