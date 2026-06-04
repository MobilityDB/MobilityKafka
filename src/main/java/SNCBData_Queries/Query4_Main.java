package SNCBData_Queries;

import functions.functions;
import functions.MeosErrorHandler;
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
import types.temporal.TInterpolation;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class Query4_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query4_Main.class);

    // STBox: Brussels-South where device 3 operates (lon 4.35-4.40, lat 50.63-50.66)
    private static final double STBOX_XMIN = 4.35;
    private static final double STBOX_XMAX = 4.40;
    private static final double STBOX_YMIN = 50.63;
    private static final double STBOX_YMAX = 50.66;
    // Full day of the SNCB dataset
    private static final String STBOX_TSPAN = "[2024-08-01 00:00:00+00, 2024-08-02 00:00:00+00]";

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query4_SNCB");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            //Close the window by force after 1 minute if no record is processed during this 1 minute
            props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "60000"); // 1 minute

            StreamsBuilder builder = new StreamsBuilder();

            TimestampExtractor myExtractor = new MyTimeStampExtractor();
            KStream<String, String> source = builder.stream("query-input", Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(myExtractor));

            source
                    .filter((key, value) -> value != null && !value.startsWith("t,")) // skip header
                    .map((key, value) -> {
                        String[] cols = value.split(",");
                        String device_id = cols[1].trim(); // device_id is the key
                        return new KeyValue<>(device_id, value); // set device_id as key, keep full row as value
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
                    // Switch here to compare the 2 different implementations:
                    .process(new Query4_Main.RestrictedTrajectoryWindowFunctionV1(STBOX_XMIN, STBOX_XMAX, STBOX_YMIN, STBOX_YMAX, STBOX_TSPAN))
                    //.process(new Query4_Main.RestrictedTrajectoryWindowFunctionV2(STBOX_XMIN, STBOX_XMAX, STBOX_YMIN, STBOX_YMAX, STBOX_TSPAN))
                    .to("query-output") ;

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


    private static class RestrictedTrajectoryWindowFunctionV1 implements ProcessorSupplier<Windowed<String>, Object, String, String> {

        private final double xmin, xmax, ymin, ymax;
        private final String tspanLiteral;

        /**
         * Parsed STBox pointer. Initialised once in init() via {@code stbox_make()}
         * and reused across all window invocations since construction is expensive and the box
         * never changes. Declared transient because JNR-FFI Pointer objects are not serialisable.
         */
        private Pointer stbox;

        private RestrictedTrajectoryWindowFunctionV1(double xmin, double xmax, double ymin, double ymax, String tspanLiteral) {
            this.xmin = xmin;
            this.xmax = xmax;
            this.ymin = ymin;
            this.ymax = ymax;
            this.tspanLiteral = tspanLiteral;
        }

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query4_Main.RestrictedTrajectoryWindowFunctionV1.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler ;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);

                    // stbox_make parameters:
                    //   hasx=true   → include spatial (XY) dimensions
                    //   hasz=false  → no Z (altitude) dimension
                    //   geodetic=true → geography/WGS-84, consistent with tgeogpoint_in
                    //   srid=4326   → WGS-84
                    //   xmin/xmax/ymin/ymax → spatial bounds (lon/lat in degrees)
                    //   zmin/zmax=0 → unused (hasz=false)
                    //   s           → temporal span pointer (tstzspan)
                    Pointer tspan = functions.tstzspan_in(tspanLiteral);
                    if (tspan == null) {
                        log.error("tstzspan_in returned null for: {}", tspanLiteral);
                        return;
                    }
                    stbox = functions.stbox_make(true, false, true, 4326,
                            xmin, xmax, ymin, ymax, 0, 0, tspan);
                    if (stbox == null) {
                        log.error("stbox_make returned null");
                    } else {
                        log.info("STBox built successfully: xmin={} xmax={} ymin={} ymax={} tspan={}",
                                xmin, xmax, ymin, ymax, tspanLiteral);
                    }
                    log.info("MEOS initialized in RestrictedTrajectoryWindowFunctionV1.init()");

                }

                @Override
                public void process(org.apache.kafka.streams.processor.api.Record<Windowed<String>, Object> record) {
                    if (stbox == null) return; // STBox failed to parse in open()

                    String device_id = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // Collect events that pass the STBox filter, sorted by timestamp.
                    List<SNCBData> surviving = new ArrayList<>();

                    for (String sncbData : rows ) {

                        SNCBData event = SNCBData.fromCsv(sncbData);
                        String ts = millisToTimestamp(event.getTimestamp());

                        // Build the tgeogpoint instant for this event.
                        String tpointWkt = String.format(
                                "POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);

                        Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                        if (tpoint == null) {
                            log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt);
                            continue;
                        }

                        // tgeo_at_stbox(lon, lat, ts, stbox)
                        // Returns null  → point is outside the STBox → skip.
                        // Returns non-null → point is inside the STBox → keep.
                        // border_inc=true means the box boundaries are inclusive ([xmin,xmax],
                        // [ymin,ymax], [tsmin,tsmax]), matching the paper's closed-interval notation.
                        Pointer restricted = functions.tgeo_at_stbox(tpoint, stbox, true);
                        if (restricted == null) {
                            log.debug("DeviceID={} skipped: point outside STBox at ts={}", device_id, ts);
                            continue;
                        }

                        surviving.add(event);
                    }

                    if (surviving.isEmpty()) return; // no event survived the STBox filter

                    // Sort by timestamp: required by tgeogpoint_in for sequence construction.
                    surviving.sort(Comparator.comparingLong(SNCBData::getTimestamp));

                    // temporal_sequence(lon, lat, ts).
                    StringBuilder seq = new StringBuilder("{");
                    for (int i = 0; i < surviving.size(); i++) {
                        SNCBData event = surviving.get(i);
                        String ts = millisToTimestamp(event.getTimestamp());
                        if (i > 0) seq.append(",");
                        seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
                    }
                    seq.append("}");

                    Pointer trajectory = functions.tgeogpoint_in(seq.toString());
                    if (trajectory == null) {
                        log.error("tgeogpoint_in returned null for sequence: {}", seq);
                        return;
                    }

                    String trajectoryEwkt = functions.tspatial_as_ewkt(trajectory, 6);

                    String output = String.format(
                            "[TRAJ][Q4] DeviceID=%-12s | points=%3d | window [%s - %s]%n | trajectory: %s",
                            device_id,
                            surviving.size(),
                            millisToTimestamp(windowStart), millisToTimestamp(windowEnd),
                            trajectoryEwkt);

                    log.info(output);
                    context.forward(new org.apache.kafka.streams.processor.api.Record<>(device_id, output, record.timestamp()));
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

    private static class RestrictedTrajectoryWindowFunctionV2 implements ProcessorSupplier<Windowed<String>, Object, String, String>{

        private final double xmin, xmax, ymin, ymax;
        private final String tspanLiteral;

        /**
         * Parsed STBox pointer. Initialised once in init() via {@code stbox_make()}
         * and reused across all window invocations since construction is expensive and the box
         * never changes. Declared transient because JNR-FFI Pointer objects are not serialisable.
         */
        private Pointer stbox;

        private RestrictedTrajectoryWindowFunctionV2(double xmin, double xmax, double ymin, double ymax, String tspanLiteral) {
            this.xmin = xmin;
            this.xmax = xmax;
            this.ymin = ymin;
            this.ymax = ymax;
            this.tspanLiteral = tspanLiteral;
        }

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query4_Main.RestrictedTrajectoryWindowFunctionV2.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler ;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);

                    Pointer tspan = functions.tstzspan_in(tspanLiteral);
                    if (tspan == null) { log.error("tstzspan_in returned null for: {}", tspanLiteral); return; }
                    stbox = functions.stbox_make(true, false, true, 4326, xmin, xmax, ymin, ymax, 0, 0, tspan);
                    if (stbox == null) log.error("stbox_make returned null");
                    else log.info("[V2] STBox built: xmin={} xmax={} ymin={} ymax={} tspan={}",
                            xmin, xmax, ymin, ymax, tspanLiteral);
                    log.info("MEOS initialized in RestrictedTrajectoryWindowFunctionV1.init()");

                }

                @Override
                public void process(org.apache.kafka.streams.processor.api.Record<Windowed<String>, Object> record) {
                    if (stbox == null) return;

                    String device_id = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // Collect and sort first: MEOS requires strictly increasing timestamps.
                    List<SNCBData> sorted = new ArrayList<>();
                    for (String sncbData : rows){
                        SNCBData event = SNCBData.fromCsv(sncbData);
                        sorted.add(event);
                    }
                    sorted.sort(Comparator.comparingLong(SNCBData::getTimestamp));
                    if (sorted.isEmpty()) return;

                    jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getSystemRuntime();
                    Pointer trajectory = null;
                    int count = 0;

                    for (SNCBData event : sorted) {
                        String wkt   = String.format("POINT(%f %f)@%s",
                                event.getLon(), event.getLat(), millisToTimestamp(event.getTimestamp()));
                        Pointer inst = functions.tgeogpoint_in(wkt);
                        if (inst == null) {
                            log.error("[V2] tgeogpoint_in returned null for DeviceID={} wkt={}", device_id, wkt);
                            continue;
                        }

                        // STBox filter: reuse the already-parsed instant pointer.
                        if (functions.tgeo_at_stbox(inst, stbox, true) == null) continue;

                        if (trajectory == null) {
                            Pointer seedArray = Memory.allocate(runtime, Long.BYTES);
                            seedArray.putPointer(0, inst);
                            trajectory = functions.tsequence_make(
                                    seedArray, 1, true, true, TInterpolation.LINEAR.getValue(), true);
                            if (trajectory == null) {
                                log.error("[V2] tsequence_make (seed) returned null for DeviceID={}", device_id);
                                return;
                            }
                        } else {
                            Pointer expanded = functions.temporal_append_tinstant(
                                    trajectory, inst, TInterpolation.LINEAR.getValue(), 0.0, null, true);
                            if (expanded == null) {
                                log.error("[V2] temporal_append_tinstant returned null for DeviceID={} wkt={}", device_id, wkt);
                                continue;
                            }
                            trajectory = expanded;
                        }
                        count++;
                    }

                    if (trajectory == null || count == 0) return;

                    String trajectoryEwkt = functions.tspatial_as_ewkt(trajectory, 6);

                    String output = String.format(
                            "[TRAJ][Q4] DeviceID=%-12s | points=%3d | window [%s - %s]%n | trajectory: %s",
                            device_id,
                            sorted.size(),
                            millisToTimestamp(windowStart), millisToTimestamp(windowEnd),
                            trajectoryEwkt);

                    log.info(output);
                    context.forward(new Record<>(device_id, output, record.timestamp()));
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
