package SNCBData_Queries;

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

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Query2_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query2_Main.class);

    private static final double VAR_FA_THRESHOLD = -10;
    private static final double VAR_FF_THRESHOLD = 10;

    /**
     * Maintenance area exclusion zones (INPolygons).
     * Placed in eastern Belgium away from the active train corridors in the dataset.
     */
    private static final String[] MAINTENANCE_AREAS_WKT = {
            "POLYGON((5.5500 50.6000, 5.5500 50.7000, 5.6500 50.7000, 5.6500 50.6000, 5.5500 50.6000))",
            "POLYGON((5.8000 49.7000, 5.8000 49.8000, 5.9000 49.8000, 5.9000 49.7000, 5.8000 49.7000))"
    };

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query2_SNCB");
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
                    //10 seconds sliding (hopping) window with 10 ms steps and a 10 seconds watermark :
                    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)).advanceBy(Duration.ofMillis(10)))
                    .aggregate(
                            () -> "",  // 1. initializer: start with empty string
                            (key, value, aggregate) -> aggregate.isEmpty() ? value : aggregate + ";" + value, // 2. aggregator: append rows separated by ";"
                            Materialized.with(Serdes.String(), Serdes.String()) // 3. serdes
                    )
                    .toStream()
                    .process(new Query2_Main.BrakeMonitoringWindowFunction( MAINTENANCE_AREAS_WKT, VAR_FA_THRESHOLD, VAR_FF_THRESHOLD))
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

    // Internal class for the processing

    public static class BrakeMonitoringWindowFunction implements ProcessorSupplier<Windowed<String>, Object, String, String> {

        private final String[] maintenanceAreasWkt;
        private final double varFaThreshold;
        private final double varFfThreshold;

        public BrakeMonitoringWindowFunction(String[] maintenanceAreasWkt, double varFaThreshold, double varFfThreshold) {
            this.maintenanceAreasWkt = maintenanceAreasWkt;
            this.varFaThreshold = varFaThreshold;
            this.varFfThreshold = varFfThreshold;
        }

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;
                private Pointer[] maintenanceZones;

                private final Logger log =
                        LoggerFactory.getLogger(Query2_Main.BrakeMonitoringWindowFunction.class);

                private static final DateTimeFormatter TIMESTAMP_FMT =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                MeosErrorHandler errorHandler ;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);

                    maintenanceZones = new Pointer[maintenanceAreasWkt.length];
                    for (int i = 0; i < maintenanceAreasWkt.length; i++) {
                        maintenanceZones[i] = functions.geog_in(maintenanceAreasWkt[i], -1);
                        if (maintenanceZones[i] == null) {
                            log.error("geog_in returned null for maintenance area {}", i + 1);
                        }
                    }
                    log.info("MEOS initialized in BrakeMonitoringWindowFunction.init(), {} maintenance zones parsed", maintenanceZones.length);
                }

                @Override
                public void process(org.apache.kafka.streams.processor.api.Record<Windowed<String>, Object> record) {
                    String device_id = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // Collect FA and FF values from events that pass the maintenance area filter.
                    List<Double> faValues = new ArrayList<>();
                    List<Double> ffValues = new ArrayList<>();

                    for (String row : rows) {
                        SNCBData sncbData = SNCBData.fromCsv(row);

                        String tsFormatted =
                                LocalDateTime.ofInstant(
                                        java.time.Instant.ofEpochMilli(sncbData.getTimestamp()),
                                        java.time.ZoneOffset.UTC
                                ).format(TIMESTAMP_FMT);

                        String tpointWkt = String.format("POINT(%f %f)@%s",
                                sncbData.getLon(), sncbData.getLat(), tsFormatted);

                        Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                        if (tpoint == null) {
                            log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt);
                            continue;
                        }

                        // Paper Line 2: eintersects_tgeo_geo(lon, lat, ts, INPolygons) == 0
                        // Skip the event if it intersects any maintenance area.
                        // eintersects_tgeo_geo returns 1 if the temporal point ever intersects the
                        // polygon, 0 otherwise. We keep only non-intersecting points (== 0).
                        boolean inMaintenanceArea = false;
                        for (Pointer zone : maintenanceZones) {
                            if (zone == null) continue;
                            if (functions.eintersects_tgeo_geo(tpoint, zone) == 1) {
                                inMaintenanceArea = true;
                                log.debug("DeviceID={} skipped: point intersects maintenance area at ts={}",
                                        device_id, tsFormatted);
                                break;
                            }
                        }
                        if (inMaintenanceArea) continue;

                        faValues.add(sncbData.getPcfaMbar());
                        ffValues.add(sncbData.getPcffMbar());

                    }

                    if (faValues.isEmpty() || ffValues.isEmpty()) return; // no surviving events in this window

                    // Paper Line 4: variation(FA) and variation(FF): statistical variance
                    double varFA = variance(faValues);
                    double varFF = variance(ffValues);

                    // Paper Line 5: varFA > 0.6 && varFF <= 0.5
                    if (varFA > varFaThreshold && varFF <= varFfThreshold) {
                        String alert = String.format(
                                "[ALERT][Q2] DeviceID=%-12s | varFA=%6.4f bar² (>%.1f) | varFF=%6.4f bar² (<=%.1f)"
                                        + " | events=%d | window [%s - %s]",
                                device_id,
                                varFA, varFaThreshold,
                                varFF, varFfThreshold,
                                faValues.size(),
                                millisToTimestamp(windowStart), millisToTimestamp(windowEnd));

                        log.warn(alert);
                        context.forward(new Record<>(device_id, alert, record.timestamp()));
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

                static double variance(List<Double> values) {
                    if (values.size() < 2) return 0.0;
                    double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                    double sumSq = values.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum();
                    return sumSq / values.size(); // population variance
                }
            };
        }
    }

}
