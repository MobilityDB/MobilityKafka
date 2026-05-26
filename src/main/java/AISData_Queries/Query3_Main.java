package AISData_Queries;

import functions.functions;
import jnr.ffi.Memory;
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
import functions.MeosErrorHandler;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import jnr.ffi.Runtime;
import types.temporal.TInterpolation;

public class Query3_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query3_Main.class);

    public static void main(String[] args) throws InterruptedException {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query3_AIS");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            StreamsBuilder builder = new StreamsBuilder();

            TimestampExtractor myExtractor = new MyTimeStampExtractor();
            KStream<String, String> source = builder.stream("query-input", Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(myExtractor));

            source
                    .filter((key, value) -> value != null && !value.startsWith("t,")) // skip header
                    .map((key, value) -> {
                        String[] cols = value.split(",");
                        String mmsi = cols[1].trim(); // mmsi is the key
                        return new KeyValue<>(mmsi, value); // set mmsi as key, keep full row as value
                    })
                    .groupByKey()
                    //10 seconds sliding (hopping) window with 10 ms steps and a 10 seconds watermark :
                    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)).advanceBy(Duration.ofMillis(10)))
                    .aggregate(
                            () -> "",  // 1. initializer: start with empty string
                            (key, value, aggregate) -> aggregate.isEmpty() ? value : aggregate + ";" + value, // 2. aggregator: append rows separated by ";"
                            Materialized.with(Serdes.String(), Serdes.String()) // 3. serdes
                    )
                    .toStream()
                    .process(new TrajectoryCreationWindowFunctionV1())
                    //.process(new TrajectoryCreationWindowFunctionV2())
                    .to("query-output") ;

            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.cleanUp();
            streams.start();

            java.lang.Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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

    // =========================================================================
    // V1: WKT StringBuilder + tgeogpoint_in
    // =========================================================================

    /**
     * Builds the entire sequence as a WKT literal {@code {POINT(lon lat)@ts,...}}
     * and parses it in one call to {@code tgeogpoint_in()}.
     */
    public static class TrajectoryCreationWindowFunctionV1 implements ProcessorSupplier<Windowed<String>, Object, String, String> {

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query3_Main.TrajectoryCreationWindowFunctionV1.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler ;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);

                    log.info("MEOS initialized in TrajectoryCreationWindowFunctionV1.init()");
                }

                @Override
                public void process(Record<Windowed<String>, Object> record) {
                    String mmsi = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // Step 1: collect and sort by timestamp.
                    // MEOS tgeogpoint_in requires instants in strictly increasing temporal order;
                    // Flink does not guarantee arrival order within a window.
                    List<AISData> sorted = new ArrayList<>();
                    for (String r : rows) sorted.add(AISData.fromCsv(r));
                    sorted.sort(Comparator.comparingLong(AISData::getTimestamp));

                    if (sorted.isEmpty()) return;

                    // Step 2 & 3: build the sequence literal: {POINT(lon lat)@ts, ...}
                    // This is the WKT representation of a tgeogpoint TSequence
                    StringBuilder seq = new StringBuilder("{");
                    for (int i = 0; i < sorted.size(); i++) {
                        AISData event = sorted.get(i);
                        String ts = millisToTimestamp(event.getTimestamp());
                        if (i > 0) seq.append(",");
                        seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
                    }
                    seq.append("}");

                    // Step 4: parse the sequence into a native MEOS tgeogpoint pointer.
                    // tgeogpoint_in accepts both single instants ("POINT(lon lat)@ts") and sequences
                    // ("{POINT(...)@ts,...}"). Here we always pass a sequence.
                    Pointer trajectory = functions.tgeogpoint_in(seq.toString());
                    if (trajectory == null) {
                        log.error("tgeogpoint_in returned null for sequence: {}", seq);
                        return;
                    }

                    // Step 5: serialise the MEOS pointer back to a human-readable WKT string.
                    // tspatial_as_ewkt(pointer, maxdd) converts any temporal spatial type to EWKT
                    // (WKT with SRID prefix), producing human-readable "POINT(lon lat)@ts" output.
                    // maxdd=6 gives 6 decimal places.
                    String trajectoryWkt = functions.tspatial_as_ewkt(trajectory, 6);

                    String output = String.format(
                            "[TRAJ][Q3] MMSI=%-12s | points=%3d | window [%s - %s]%n | trajectory: %s",
                            mmsi,
                            sorted.size(),
                            millisToTimestamp(windowStart), millisToTimestamp(windowEnd),
                            trajectoryWkt);

                    log.info(output);
                    context.forward(new Record<>(mmsi, output, record.timestamp()));

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

    // =========================================================================
    // V2: Expand: tgeogpoint_in (instant) → temporal_append_tinstant
    // =========================================================================

    /**
     * Builds the sequence incrementally: the first instant seeds the sequence via
     * {@code tsequence_make}, and subsequent instants are appended with
     * {@code temporal_append_tinstant()}. MEOS handles capacity doubling internally.
     */
    public static class TrajectoryCreationWindowFunctionV2 implements ProcessorSupplier<Windowed<String>, Object, String, String> {

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query3_Main.TrajectoryCreationWindowFunctionV2.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler ;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);

                    log.info("MEOS initialized in TrajectoryCreationWindowFunctionV2.init()");
                }


                @Override
                public void process(Record<Windowed<String>, Object> record) {
                    String mmsi = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    List<AISData> sorted = new ArrayList<>();
                    for (String r : rows) sorted.add(AISData.fromCsv(r));
                    sorted.sort(Comparator.comparingLong(AISData::getTimestamp));
                    if (sorted.isEmpty()) return;

                    Runtime runtime = Runtime.getSystemRuntime();
                    Pointer trajectory = null;
                    int count = 0;

                    for (AISData aisData : sorted) {
                        String wkt   = String.format("POINT(%f %f)@%s",
                                aisData.getLon(), aisData.getLat(), millisToTimestamp(aisData.getTimestamp()));
                        Pointer inst = functions.tgeogpoint_in(wkt);
                        if (inst == null) {
                            log.error("[V2] tgeogpoint_in returned null for DeviceID={} wkt={}", mmsi, wkt);
                            continue;
                        }

                        if (trajectory == null) {
                            // Seed the expandable sequence with the first instant.
                            Pointer seedArray = Memory.allocate(runtime, Long.BYTES);
                            seedArray.putPointer(0, inst);
                            trajectory = functions.tsequence_make(
                                    seedArray, 1, true, true, TInterpolation.LINEAR.getValue(), true);
                            if (trajectory == null) {
                                log.error("[V2] tsequence_make (seed) returned null for DeviceID={}", mmsi);
                                return;
                            }
                        } else {
                            // Append: MEOS expands capacity as needed.
                            Pointer expanded = functions.temporal_append_tinstant(
                                    trajectory, inst, TInterpolation.LINEAR.getValue(), 0.0, null, true);
                            if (expanded == null) {
                                log.error("[V2] temporal_append_tinstant returned null for DeviceID={} wkt={}", mmsi, wkt);
                                continue;
                            }
                            trajectory = expanded;
                        }
                        count++;
                    }
                    if (trajectory == null || count == 0) return;
                    String trajectoryEwkt = functions.tspatial_as_ewkt(trajectory, 6);

                    String output = String.format(
                            "[TRAJ][Q3] MMSI=%-12s | points=%3d | window [%s - %s]%n | trajectory: %s",
                            mmsi,
                            sorted.size(),
                            millisToTimestamp(windowStart), millisToTimestamp(windowEnd),
                            trajectoryEwkt);

                    log.info(output);
                    context.forward(new Record<>(mmsi, output, record.timestamp()));

                }

                @Override
                public void close() {
                    Processor.super.close();
                    functions.meos_finalize();
                }

                private String millisToTimestamp(long millis) {
                    return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
                }
            };
        }


    }
}
