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

public class Query7_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query7_Main.class);

    private static final int TOP_K = 10;

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query7_AIS");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            //Close the window by force after 1 minute if no record is processed during this 1 minute
            props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "60000"); // 1 minute

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

            result.process(new Query7_Main.ClosestPairsCoGroupFunction(TOP_K))
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

    // Internal class for the join process
    public static class ClosestPairsCoGroupFunction implements ProcessorSupplier<String, Object, String, String> {

        private final int topK;

        public ClosestPairsCoGroupFunction(int topK) {
            this.topK = topK;
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

                    //Hashmap to delete duplicates
                    Map<String, double[]> pairMap = new HashMap<>();

                    for (int i = 0; i < lefts.size(); i++) {
                        AISData left    = lefts.get(i);
                        Pointer geoLeft = geoLefts.get(i);
                        if (geoLeft == null) continue;

                        for (int j = 0; j < rights.size(); j++) {
                            AISData right    = rights.get(j);
                            Pointer geoRight = geoRights.get(j);
                            if (geoRight == null) continue;

                            // device_id < device_id2
                            if (left.getMmsi() >= right.getMmsi()) continue;

                            double dist = functions.geog_distance(geoLeft, geoRight);

                            // Keep only the minimum distance per unique (mmsi1, mmsi2) pair.
                            String key = left.getMmsi() + ":" + right.getMmsi();
                            if (!pairMap.containsKey(key) || dist < pairMap.get(key)[2]) {
                                pairMap.put(key, new double[]{
                                        left.getMmsi(), right.getMmsi(), dist,
                                        left.getLon(), left.getLat(),
                                        right.getLon(), right.getLat()});
                            }
                        }
                    }

                    if (pairMap.isEmpty()) return;

                    //Keep the top K smallest pairs
                    List<double[]> pairs = new ArrayList<>(pairMap.values());
                    pairs.sort(Comparator.comparingDouble(e -> e[2]));

                    int emitCount = Math.min(topK, pairs.size());
                    for (int rank = 0; rank < emitCount; rank++) {
                        double[] p = pairs.get(rank);

                        String result = String.format(
                                "[TOPK][Q7] rank=%2d/%d | MMSI1=%-12d (lon=%9.5f lat=%8.5f)"
                                        + " | MMSI2=%-12d (lon=%9.5f lat=%8.5f)"
                                        + " | mindist=%10.3f m",
                                rank + 1, emitCount,
                                (long) p[0], p[3], p[4],
                                (long) p[1], p[5], p[6],
                                p[2]);

                        String mmsis = String.valueOf(p[0])+String.valueOf(p[1]);
                        log.info(result);
                        context.forward(new Record<>(mmsis, result, record.timestamp()));
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
