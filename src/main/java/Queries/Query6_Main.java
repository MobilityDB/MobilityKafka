package Queries;

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
import java.util.Properties;

public class Query6_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query6_Main.class);

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query6_AIS");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            StreamsBuilder builder = new StreamsBuilder();

            TimestampExtractor myExtractor = new MyTimeStampExtractor();

            KStream<String, String> stream1 = builder.stream("query-input", Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(myExtractor));

            KStream<String, String> gps = stream1
                    .filter((key, value) -> value != null && !value.startsWith("t,"))
                    .map((key, value) -> {
                        String[] cols = value.split(",");
                        return new KeyValue<>(cols[1].trim(), value); // mmsi as key
                    });

            KStream<String, String> stream2 = builder.stream("query-input", Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(myExtractor));

            KStream<String, String> gps2 = stream2
                    .filter((key, value) -> value != null && !value.startsWith("t,"))
                    .map((key, value) -> {
                        String[] cols = value.split(",");
                        return new KeyValue<>(cols[1].trim(), value); // mmsi as key
                    });

            KStream<String, String> join = gps.join(gps2,
                            (value1, value2) -> value1 + ";" + value2, // ValueJoiner : concatenation
                            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
                            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                    );

            join.process(new Query6_Main.NearestApproachJoinFunction())
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
    public static class NearestApproachJoinFunction implements ProcessorSupplier<String, Object, String, String> {

        @Override
        public Processor<String, Object, String, String> get() {
            return new Processor<String, Object, String, String>() {

                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query6_Main.NearestApproachJoinFunction.class);

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

                    AISData left = AISData.fromCsv(rows[0]);
                    AISData right = AISData.fromCsv(rows[1]);

                    String tsLeft  = millisToTimestamp(left.getTimestamp());
                    String tsRight = millisToTimestamp(right.getTimestamp());

                    // Build tgeogpoint instants for both sides of the pair.
                    Pointer tpointLeft = functions.tgeogpoint_in(
                            String.format("POINT(%f %f)@%s", left.getLon(),  left.getLat(),  tsLeft));
                    Pointer tpointRight = functions.tgeogpoint_in(
                            String.format("POINT(%f %f)@%s", right.getLon(), right.getLat(), tsRight));

                    if (tpointLeft == null || tpointRight == null) {
                        log.error("tgeogpoint_in returned null for pair MMSI={}", left.getMmsi());
                        return ;
                    }

                    Pointer geoLeft  = functions.tgeo_end_value(tpointLeft);
                    Pointer geoRight = functions.tgeo_end_value(tpointRight);

                    if (geoLeft == null || geoRight == null) {
                        log.error("temporal_end_value returned null for MMSI={}", left.getMmsi());
                        return ;
                    }

                    double mindist = functions.geog_distance(geoLeft, geoRight);

                    // Paper Line 5: filter(lat > 0.0): keep only pairs where left-side lat is positive.
                    // All AIS positions in this dataset are in the Northern Hemisphere (~55–58°N),
                    // so all pairs pass this filter.
                    if (left.getLat() <= 0.0) return ;

                    String result = String.format(
                            "[MINDIST][Q6] MMSI=%-12d"
                                    + " | left(lon=%10.5f lat=%9.5f ts=%s)"
                                    + " | right(lon=%10.5f lat=%9.5f ts=%s)"
                                    + " | mindist=%12.3f m",
                            left.getMmsi(),
                            left.getLon(),  left.getLat(),  tsLeft,
                            right.getLon(), right.getLat(), tsRight,
                            mindist);

                    log.info(result);
                    context.forward(new Record<>(String.valueOf(left.getMmsi()), result, record.timestamp()));

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
