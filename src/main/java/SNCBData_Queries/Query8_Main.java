package SNCBData_Queries;

import functions.functions;
import functions.MeosErrorHandler;
import functions.error_handler_fn;
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
import java.util.List;
import java.util.Properties;

public class Query8_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query8_Main.class);
    // EKF parameters: degree² units (C reference example defaults: https://github.com/marianaGarcez/MobilityDB/blob/e3319c1e6fc9157d19cb0580e097224cd42a8214/meos/examples/ais_ekf_clean.c)
    private static final double  GATE          = 8.0;
    private static final double  Q             = 5e-10;
    private static final double  R             = 4e-6;
    private static final boolean DROP_OUTLIERS = false;

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query8_SNCB");
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
                        String deviceID = cols[1].trim(); // deviceID is the key
                        return new KeyValue<>(deviceID, value); // set deviceID as key, keep full row as value
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
                    .process(new Query8_Main.MeosEkfWindowFunction(GATE, Q, R, DROP_OUTLIERS))
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

    public static class MeosEkfWindowFunction implements ProcessorSupplier<Windowed<String>, Object, String, String> {
        private static final Logger log = LoggerFactory.getLogger(Query8_Main.MeosEkfWindowFunction.class);

        private final double gate;
        private final double q;
        private final double r;
        private final boolean dropOutliers;

        private transient error_handler_fn errorHandler;

        public MeosEkfWindowFunction(double gate, double q, double r, boolean dropOutliers) {
            this.gate = gate;
            this.q = q;
            this.r = r;
            this.dropOutliers = dropOutliers;
        }

        @Override
        public Processor<Windowed<String>, Object, String, String> get() {
            return new Processor<Windowed<String>, Object, String, String>() {
                private ProcessorContext<String, String> context;

                private final Logger log =
                        LoggerFactory.getLogger(Query8_Main.MeosEkfWindowFunction.class);

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
                public void process(org.apache.kafka.streams.processor.api.Record<Windowed<String>, Object> record) {
                    String deviceID = record.key().key();
                    long windowStart = record.key().window().start();
                    long windowEnd = record.key().window().end();

                    String[] rows = record.value().toString().split(";");

                    // 1. Collect and sort
                    List<SNCBData> sorted = new ArrayList<>();
                    for (String sncbData : rows){
                        SNCBData event = SNCBData.fromCsv(sncbData);
                        sorted.add(event);
                    }
                    sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
                    if (sorted.isEmpty()) return;

                    jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getSystemRuntime();

                    // 2. Single-point case: EKF not applicable, emit raw point directly.
                    if (sorted.size() < 2) {
                        SNCBData event = sorted.get(0);
                        String wkt   = String.format("POINT(%f %f)@%s",
                                event.getLon(), event.getLat(), millisToTimestamp(event.getTimestamp()));
                        Pointer inst = functions.tgeompoint_in(wkt);
                        if (inst == null) return;
                        Pointer seedArray = Memory.allocate(runtime, Long.BYTES);
                        seedArray.putPointer(0, inst);
                        Pointer singleSeq = functions.tsequence_make(
                                seedArray, 1, true, true, TInterpolation.LINEAR.getValue(), true);
                        if (singleSeq == null) return;
                        String result = String.format(
                                "[EKF-V2][Q8] DeviceID=%-12s | points=1 | EKF skipped (single point)"
                                        + " | window [%s - %s]%n"
                                        + "             raw=cleaned: %s",
                                deviceID, windowStart, windowEnd,
                                functions.tspatial_as_ewkt(singleSeq, 6));
                        log.info(result);
                        context.forward(new org.apache.kafka.streams.processor.api.Record<>(deviceID, result, record.timestamp()));
                        return;
                    }

                    // 3. Build tgeompoint instants via tgeompoint_in (WKT string).
                    List<Pointer> instants = new ArrayList<>(sorted.size());
                    for (SNCBData event : sorted) {
                        String ts   = millisToTimestamp(event.getTimestamp());
                        String wkt  = String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);
                        Pointer inst = functions.tgeompoint_in(wkt);
                        if (inst == null) {
                            log.error("[EKF-V2] tgeompoint_in returned null for DeviceID={} wkt={}", deviceID, wkt);
                            continue;
                        }
                        instants.add(inst);
                    }
                    if (instants.size() < 2) return;

                    // 4. Assemble raw tgeompoint sequence.
                    Pointer ptrArray = Memory.allocate(runtime, Math.toIntExact((long) instants.size() * Long.BYTES));
                    for (int i = 0; i < instants.size(); i++) {
                        ptrArray.putPointer((long) i * Long.BYTES, instants.get(i));
                    }
                    Pointer rawSeq = functions.tsequence_make(
                            ptrArray, instants.size(), true, true, TInterpolation.LINEAR.getValue(), true);
                    if (rawSeq == null) {
                        log.error("[EKF-V2] tsequence_make returned null for DeviceID={}", deviceID);
                        return;
                    }

                    // 5. Apply MEOS native Extended Kalman Filter.
                    Pointer cleanedSeq = functions.temporal_ext_kalman_filter(rawSeq, gate, q, r, dropOutliers);
                    if (cleanedSeq == null) {
                        log.warn("[EKF-V2] temporal_ext_kalman_filter returned null for DeviceID={} "
                                + "(window too small?). Falling back to raw.", deviceID);
                        cleanedSeq = rawSeq;
                    }

                    String result = String.format(
                            "[EKF-V2][Q8] DeviceID=%-12s | points=%2d | gate=%.1f q=%.2e r=%.2e drop=%b"
                                    + " | window [%s - %s]%n"
                                    + "             raw:     %s%n"
                                    + "             cleaned: %s",
                            deviceID, instants.size(), gate, q, r, dropOutliers,
                            millisToTimestamp(windowStart), millisToTimestamp(windowEnd),
                            functions.tspatial_as_ewkt(rawSeq, 6),
                            functions.tspatial_as_ewkt(cleanedSeq, 6));

                    log.info(result);
                    context.forward(new Record<>(deviceID, result, record.timestamp()));
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
