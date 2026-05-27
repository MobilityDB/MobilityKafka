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
import java.util.Properties;

public class Query1_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query1_Main.class);

    /** Distance threshold in metres: paper specifies 20 m for SNCB trains. */
    private static final double ALERT_DISTANCE_METERS = 20.0;

    /**
     * High-risk zones along the Belgian railway network (INPolygons).
     *
     * Zone 1 - Brussels-South area
     * Zone 2 - Brussels-North / Schaerbeek area
     * Zone 3 - Ghent area
     * Zone 4 - Antwerp area
     */
    private static final String[] HIGH_RISK_ZONES_WKT = {
            "POLYGON((4.3550 50.6350, 4.3550 50.6550, 4.3750 50.6550, 4.3750 50.6350, 4.3550 50.6350))",
            "POLYGON((4.3500 50.8600, 4.3500 50.8800, 4.3700 50.8800, 4.3700 50.8600, 4.3500 50.8600))",
            "POLYGON((3.7100 51.0200, 3.7100 51.0400, 3.7300 51.0400, 3.7300 51.0200, 3.7100 51.0200))",
            "POLYGON((4.4100 51.2900, 4.4100 51.3100, 4.4300 51.3100, 4.4300 51.2900, 4.4100 51.2900))"
    };

    public static void main(String[] args) throws InterruptedException {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query1_SNCB");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            StreamsBuilder builder = new StreamsBuilder();

            TimestampExtractor myExtractor = new MyTimeStampExtractor() ;
            KStream<String, String> source = builder.stream("query-input", Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(myExtractor));

            source
                    .map((key, value) -> {
                        String[] cols = value.split(",");
                        String device_id = cols[1].trim(); // device_id is the key
                        return new KeyValue<>(device_id, value); // set device_id as key, keep full row as value
                    })
                    .groupByKey()
                    //10 seconds tumbling window with a 10 seconds watermark :
                    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)))
                    .aggregate(
                            () -> "",  // 1. initializer: start with empty string
                            (key, value, aggregate) -> aggregate.isEmpty() ? value : aggregate + ";" + value, // 2. aggregator: append rows separated by ";"
                            Materialized.with(Serdes.String(), Serdes.String()) // 3. serdes
                    )
                    .toStream()
                    .process(new Query1_Main.HighRiskZoneWindowFunction(HIGH_RISK_ZONES_WKT, ALERT_DISTANCE_METERS))
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

    // Internal class for the zone window
    public static class HighRiskZoneWindowFunction implements ProcessorSupplier<Windowed<String>, Object, String, String> {

        private final String[] zoneWkt;
        private final double distanceMeters;


        public HighRiskZoneWindowFunction(String[] zoneWkt, double distanceMeters) {
            this.zoneWkt = zoneWkt;
            this.distanceMeters = distanceMeters;
        }

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {

                private ProcessorContext<String, String> context;
                private Pointer[] hazardZones;

                private final Logger log =
                        LoggerFactory.getLogger(Query1_Main.HighRiskZoneWindowFunction.class);

                private MeosErrorHandler errorHandler;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                    errorHandler = new MeosErrorHandler();
                    functions.meos_initialize_timezone("UTC");
                    functions.meos_initialize_error_handler(errorHandler);

                    this.hazardZones = new Pointer[zoneWkt.length];
                    for (int i = 0; i < zoneWkt.length; i++) {
                        hazardZones[i] = functions.geog_in(zoneWkt[i], -1);
                        if (hazardZones[i] == null) {
                            log.error("geog_in returned null for ZONE {}", i + 1);
                        }
                    }
                    log.info("MEOS initialized in HighRiskZoneWindowFunction.init(), {} hazard zones parsed", hazardZones.length);
                }
                @Override
                public void process(org.apache.kafka.streams.processor.api.Record<Windowed<String>, Object> record) {
                    String device_id = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    for (String row : rows) {
                        SNCBData sncbData = SNCBData.fromCsv(row);

                        String tsFormatted =
                                LocalDateTime.ofInstant(
                                        java.time.Instant.ofEpochMilli(sncbData.getTimestamp()),
                                        java.time.ZoneOffset.UTC
                                ).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                        String tpointWkt = String.format("POINT(%f %f)@%s",
                                sncbData.getLon(), sncbData.getLat(), tsFormatted);

                        Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                        if (tpoint == null) {
                            log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt);
                            continue;
                        }

                        for (int i = 0; i < hazardZones.length; i++) {
                            if (hazardZones[i] == null) continue;

                            // edwithin_tgeo_geo returns 1 if tpoint is within distanceMeters of the
                            // zone polygon at any instant — implements paper Line 2.
                            int within = functions.edwithin_tgeo_geo(
                                    tpoint, hazardZones[i], distanceMeters);

                            if (within == 1) {
                                String alert = String.format(
                                        "[ALERT][Q1] DeviceID=%-12s | lon=%10.5f lat=%9.5f"
                                                + " | ts=%s | within %.0f m of ZONE %d"
                                                + " | window [%s - %s]",
                                        device_id,
                                        sncbData.getLon(), sncbData.getLat(),
                                        tsFormatted,
                                        distanceMeters,
                                        i + 1,
                                        millisToTimestamp(windowStart), millisToTimestamp(windowEnd));


                                // if alert, forward to output
                                log.warn(alert);
                                context.forward(new Record<>(device_id, alert, record.timestamp()));
                            }
                        }
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
                    return dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }
            };

        }
    }

}
