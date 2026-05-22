package Queries;

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

    // Spatial bounds of the restricted zone (WGS-84 degrees).
    // Covers the Esbjerg / North Sea area where MMSI 566948000 operates.
    private static final double STBOX_XMIN = 4.48;
    private static final double STBOX_XMAX = 4.64;
    private static final double STBOX_YMIN = 55.55;
    private static final double STBOX_YMAX = 55.66;

    /**
     * Temporal bounds of the restricted zone as a MobilityDB tstzspan literal.
     * Covers the full day of the AIS dataset (2021-01-08).
     * Parsed by {@code tstzspan_in()} and passed to {@code stbox_make()}.
     *
     * The limits of the STBOX should let only the 566948000 MMSI ship pass and only its coordinates will be
     *      used to build the trajectory
     *
     * The 3 other ones should never appear since they don't fulfill the STBOX filter
     *          265513270
     *          219027804
     *          219001559
     */
    private static final String STBOX_TSPAN = "[2021-01-08 00:00:00+00, 2021-01-09 00:00:00+00]";


    public static void main(String[] args) throws InterruptedException {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query4_AIS");
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
                    // Switch here to compare the 2 different implementations:
                    //.process(new Query4_Main.RestrictedTrajectoryWindowFunctionV1(STBOX_XMIN, STBOX_XMAX, STBOX_YMIN, STBOX_YMAX, STBOX_TSPAN))
                    .process(new Query4_Main.RestrictedTrajectoryWindowFunctionV2(STBOX_XMIN, STBOX_XMAX, STBOX_YMIN, STBOX_YMAX, STBOX_TSPAN))
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

    private static class RestrictedTrajectoryWindowFunctionV1 implements ProcessorSupplier<Windowed<String>, Object, String, String>{

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
                public void process(Record<Windowed<String>, Object> record) {
                    if (stbox == null) return; // STBox failed to parse in open()

                    String mmsi = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // Collect events that pass the STBox filter, sorted by timestamp.
                    List<AISData> surviving = new ArrayList<>();

                    for (String aisData : rows ) {

                        AISData event = AISData.fromCsv(aisData);
                        String ts = millisToTimestamp(event.getTimestamp());

                        // Build the tgeogpoint instant for this event.
                        String tpointWkt = String.format(
                                "POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);

                        Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                        if (tpoint == null) {
                            log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt);
                            continue;
                        }

                        // Paper Line 2: tgeo_at_stbox(lon, lat, ts, stbox)
                        // Returns null  → point is outside the STBox → skip.
                        // Returns non-null → point is inside the STBox → keep.
                        // border_inc=true means the box boundaries are inclusive ([xmin,xmax],
                        // [ymin,ymax], [tsmin,tsmax]), matching the paper's closed-interval notation.
                        Pointer restricted = functions.tgeo_at_stbox(tpoint, stbox, true);
                        if (restricted == null) {
                            log.debug("MMSI={} skipped: point outside STBox at ts={}", mmsi, ts);
                            continue;
                        }

                        surviving.add(event);
                    }

                    if (surviving.isEmpty()) return; // no event survived the STBox filter

                    // Sort by timestamp: required by tgeogpoint_in for sequence construction.
                    surviving.sort(Comparator.comparingLong(AISData::getTimestamp));

                    // Paper Line 4: temporal_sequence(lon, lat, ts).
                    StringBuilder seq = new StringBuilder("{");
                    for (int i = 0; i < surviving.size(); i++) {
                        AISData event = surviving.get(i);
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
                            "[TRAJ][Q4] MMSI=%-12s | points=%3d | window [%s - %s]%n | trajectory: %s",
                            mmsi,
                            surviving.size(),
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
                public void process(Record<Windowed<String>, Object> record) {
                    if (stbox == null) return;

                    String mmsi = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // Collect and sort first: MEOS requires strictly increasing timestamps.
                    List<AISData> sorted = new ArrayList<>();
                    for (String aisData : rows){
                        AISData event = AISData.fromCsv(aisData);
                        sorted.add(event);
                    }
                    sorted.sort(Comparator.comparingLong(AISData::getTimestamp));
                    if (sorted.isEmpty()) return;

                    jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getSystemRuntime();
                    Pointer trajectory = null;
                    int count = 0;

                    for (AISData event : sorted) {
                        String wkt   = String.format("POINT(%f %f)@%s",
                                event.getLon(), event.getLat(), millisToTimestamp(event.getTimestamp()));
                        Pointer inst = functions.tgeogpoint_in(wkt);
                        if (inst == null) {
                            log.error("[V2] tgeogpoint_in returned null for DeviceID={} wkt={}", mmsi, wkt);
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
                                log.error("[V2] tsequence_make (seed) returned null for DeviceID={}", mmsi);
                                return;
                            }
                        } else {
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
                            "[TRAJ][Q4] MMSI=%-12s | points=%3d | window [%s - %s]%n | trajectory: %s",
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
