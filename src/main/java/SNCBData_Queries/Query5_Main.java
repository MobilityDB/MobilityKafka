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

public class Query5_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query5_Main.class);

    /** Conversion factor from km/h (gps_speed unit) to m/s. */
    private static final double KMH_TO_MS = 1.0 / 3.6;

    /** avg_speed > 50 m/s (~180 km/h) */
    private static final double AVG_SPEED_THRESHOLD_MS = 50.0;

    /** min_speed > 20 m/s (~72 km/h) */
    private static final double MIN_SPEED_THRESHOLD_MS = 20.0;

    /** Brussels area geofence polygon */
    private static final String GEOFENCE_WKT =
            "POLYGON((4.32 50.60, 4.32 50.72, 4.48 50.72, 4.48 50.60, 4.32 50.60))";

    /** Distance for edwithin_tgeo_geo */
    private static final double GEOFENCE_DISTANCE_METERS = 1.0;

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query5_SNCB");
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
                        String device_id = cols[1].trim(); // device_id is the key
                        return new KeyValue<>(device_id, value); // set device_id as key, keep full row as value
                    })
                    .groupByKey()
                    //45 seconds sliding (hopping) window with 5 seconds steps and a 10 seconds watermark :
                    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(5)))
                    .aggregate(
                            () -> "",  // 1. initializer: start with empty string
                            (key, value, aggregate) -> aggregate.isEmpty() ? value : aggregate + ";" + value, // 2. aggregator: append rows separated by ";"
                            Materialized.with(Serdes.String(), Serdes.String()) // 3. serdes
                    )
                    .toStream()
                    // Switch here to compare the 2 different implementations:
                    .process(new Query5_Main.HighSpeedAlertV1_WKT(GEOFENCE_WKT, GEOFENCE_DISTANCE_METERS, AVG_SPEED_THRESHOLD_MS, MIN_SPEED_THRESHOLD_MS))
                    //.process(new Query5_Main.HighSpeedAlertV2_Expand(GEOFENCE_WKT, GEOFENCE_DISTANCE_METERS, AVG_SPEED_THRESHOLD_MS, MIN_SPEED_THRESHOLD_MS))
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

    private static class HighSpeedAlertV1_WKT implements ProcessorSupplier<Windowed<String>, Object, String, String> {

        private final String geofenceWkt;
        private final double geofenceDistMeters;
        private final double avgSpeedThresholdMs;
        private final double minSpeedThresholdMs;

        /**
         * Geofence polygon pointer, parsed once in init() for reuse across all windows.
         * Declared transient because JNR-FFI Pointer objects are not serialisable.
         */
        private Pointer geofence;

        private HighSpeedAlertV1_WKT(String geofenceWkt, double geofenceDistMeters, double avgSpeedThresholdMs, double minSpeedThresholdMs) {
            this.geofenceWkt = geofenceWkt;
            this.geofenceDistMeters = geofenceDistMeters;
            this.avgSpeedThresholdMs = avgSpeedThresholdMs;
            this.minSpeedThresholdMs = minSpeedThresholdMs;
        }

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query5_Main.HighSpeedAlertV1_WKT.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);

                    // geog_in(wkt, -1) → SRID=4326 geography, consistent with tgeogpoint_in.
                    geofence = functions.geog_in(geofenceWkt, -1);
                    if (geofence == null) {
                        log.error("geog_in returned null for geofence: {}", geofenceWkt);
                    } else {
                        log.info("Geofence polygon parsed successfully: {}", geofenceWkt);
                    }
                    log.info("MEOS initialized in HighSpeedAlertV1_WKT.init()");
                }

                @Override
                public void process(org.apache.kafka.streams.processor.api.Record<Windowed<String>, Object> record) {
                    if (geofence == null) return;

                    String device_id = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // Collect events that pass the geofence filter, sorted by timestamp.
                    List<SNCBData> surviving = new ArrayList<>();

                    for (String sncbData : rows) {

                        SNCBData event = SNCBData.fromCsv(sncbData);
                        String ts = millisToTimestamp(event.getTimestamp());

                        Pointer tpoint = functions.tgeogpoint_in(
                                String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
                        if (tpoint == null) {
                            log.error("tgeogpoint_in returned null for DeviceID={} at ts={}", device_id, ts);
                            continue;
                        }

                        // Paper Line 2: edwithin_tgeo_geo(lon, lat, ts, POLYGON, 1) == 1
                        // Distance=1m is effectively an intersection test.
                        if (functions.edwithin_tgeo_geo(tpoint, geofence, geofenceDistMeters) != 1) {
                            log.debug("DeviceID={} skipped: outside geofence at ts={}", device_id, ts);
                            continue;
                        }

                        surviving.add(event);
                    }

                    if (surviving.isEmpty()) return;

                    surviving.sort(Comparator.comparingLong(SNCBData::getTimestamp));

                    // Paper Line 5: three aggregations computation

                    // (a) temporal_sequence(lon, lat, ts).
                    StringBuilder seq = new StringBuilder("{");
                    double speedSumMs = 0.0;
                    double minSpeedMs = Double.MAX_VALUE;

                    for (int i = 0; i < surviving.size(); i++) {
                        SNCBData event = surviving.get(i);
                        String ts = millisToTimestamp(event.getTimestamp());

                        if (i > 0) seq.append(",");
                        seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));

                        // (b) avg(gps_speed) and (c) min(gps_speed):
                        // Convert knots → m/s before accumulating.
                        double speedMs = event.getGpsSpeed() * KMH_TO_MS;
                        speedSumMs += speedMs;
                        if (speedMs < minSpeedMs) minSpeedMs = speedMs;
                    }
                    seq.append("}");

                    double avgSpeedMs = speedSumMs / surviving.size();

                    // Paper Line 6: (avg_speed > 50) || (min_speed > 20)
                    // OR condition: alert if either aggregate crosses its threshold.
                    if (avgSpeedMs <= avgSpeedThresholdMs && minSpeedMs <= minSpeedThresholdMs) return;

                    // Build trajectory for output.
                    Pointer trajectory = functions.tgeogpoint_in(seq.toString());
                    String trajectoryEwkt = (trajectory != null)
                            ? functions.tspatial_as_ewkt(trajectory, 6)
                            : seq.toString(); // fallback to raw WKT if MEOS parse fails

                    String triggerReason = buildTriggerReason(avgSpeedMs, minSpeedMs);

                    String alert = String.format(
                            "[ALERT][Q5] DeviceID=%-12s | avgSpeed=%6.2f m/s (>%.1f) | minSpeed=%6.2f m/s (>%.1f)"
                                    + " | points=%d | trigger=%s | window [%s - %s]%n"
                                    + "             trajectory: %s",
                            device_id,
                            avgSpeedMs, avgSpeedThresholdMs,
                            minSpeedMs, minSpeedThresholdMs,
                            surviving.size(),
                            triggerReason,
                            millisToTimestamp(windowStart), millisToTimestamp(windowEnd),
                            trajectoryEwkt);

                    log.warn(alert);
                    context.forward(new org.apache.kafka.streams.processor.api.Record<>(device_id, alert, record.timestamp()));

                }

                @Override
                public void close() {
                    Processor.super.close();
                    functions.meos_finalize();
                }

                private String millisToTimestamp(long millis) {
                    return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
                }

                /**
                 * Describes which condition(s) of the Line 6 OR filter triggered the alert.
                 * Helps distinguish at a glance whether the alert was caused by high average speed,
                 * high minimum speed, or both simultaneously.
                 */
                private String buildTriggerReason(double avgSpeedMs, double minSpeedMs) {
                    boolean avgTriggered = avgSpeedMs > avgSpeedThresholdMs;
                    boolean minTriggered = minSpeedMs > minSpeedThresholdMs;
                    if (avgTriggered && minTriggered) return "AVG+MIN";
                    if (avgTriggered) return "AVG";
                    return "MIN";
                }
            };
        }
    }

    // =========================================================================
    // V2: Geofence filter + tgeogpoint_in (instant) → temporal_append_tinstant
    // =========================================================================

    /**
     * Filters events through the geofence and builds the surviving trajectory
     * incrementally using {@code temporal_append_tinstant()}.
     */
    public static class HighSpeedAlertV2_Expand implements ProcessorSupplier<Windowed<String>, Object, String, String> {

        private final String geofenceWkt;
        private final double geofenceDistMeters;
        private final double avgSpeedThresholdMs;
        private final double minSpeedThresholdMs;

        /**
         * Geofence polygon pointer, parsed once in init() for reuse across all windows.
         * Declared transient because JNR-FFI Pointer objects are not serialisable.
         */
        private Pointer geofence;

        public HighSpeedAlertV2_Expand(String geofenceWkt, double geofenceDistMeters, double avgSpeedThresholdMs, double minSpeedThresholdMs) {
            this.geofenceWkt = geofenceWkt;
            this.geofenceDistMeters = geofenceDistMeters;
            this.avgSpeedThresholdMs = avgSpeedThresholdMs;
            this.minSpeedThresholdMs = minSpeedThresholdMs;
        }

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query5_Main.HighSpeedAlertV2_Expand.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);
                    geofence = functions.geog_in(geofenceWkt, -1);
                    if (geofence == null) log.error("[V2] geog_in returned null for geofence: {}", geofenceWkt);
                    log.info("MEOS initialized in HighSpeedAlertV2_Expand.init()");
                }

                @Override
                public void process(org.apache.kafka.streams.processor.api.Record<Windowed<String>, Object> record) {
                    if (geofence == null) return;

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
                    double speedSumMs  = 0.0;
                    double minSpeedMs  = Double.MAX_VALUE;
                    int count          = 0;

                    for (SNCBData event : sorted) {
                        String wkt    = String.format("POINT(%f %f)@%s",
                                event.getLon(), event.getLat(), millisToTimestamp(event.getTimestamp()));
                        Pointer inst = functions.tgeogpoint_in(wkt);
                        if (inst == null) {
                            log.error("[V2] tgeogpoint_in returned null for DeviceID={} wkt={}", device_id, wkt);
                            continue;
                        }

                        // Geofence filter: reuse the already-parsed instant pointer.
                        if (functions.edwithin_tgeo_geo(inst, geofence, geofenceDistMeters) != 1) continue;

                        double speedMs = event.getGpsSpeed() * KMH_TO_MS;
                        speedSumMs += speedMs;
                        if (speedMs < minSpeedMs) minSpeedMs = speedMs;

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

                    double avgSpeedMs = speedSumMs / count;
                    if (avgSpeedMs <= avgSpeedThresholdMs && minSpeedMs <= minSpeedThresholdMs) return;

                    String trajectoryEwkt = functions.tspatial_as_ewkt(trajectory, 6);
                    String triggerReason = (avgSpeedMs > avgSpeedThresholdMs && minSpeedMs > minSpeedThresholdMs)
                            ? "AVG+MIN" : (avgSpeedMs > avgSpeedThresholdMs ? "AVG" : "MIN");

                    String alert = String.format(
                            "[ALERT][Q5][V2] DeviceID=%-12s | avgSpeed=%6.2f m/s (>%.1f)"
                                    + " | minSpeed=%6.2f m/s (>%.1f) | points=%d | trigger=%s"
                                    + " | window [%s - %s]%n"
                                    + "             trajectory: %s",
                            device_id, avgSpeedMs, avgSpeedThresholdMs, minSpeedMs, minSpeedThresholdMs,
                            count, triggerReason, millisToTimestamp(windowStart), millisToTimestamp(windowEnd), trajectoryEwkt);

                    log.warn(alert);
                    context.forward(new Record<>(device_id, alert, record.timestamp()));
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
