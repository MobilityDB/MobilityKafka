package berlinmod;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q1 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Which vehicles have appeared in the stream?"</i>
 *
 * <p>Uses a {@link KeyValueStore} keyed by {@code vehicleId} (named in the
 * caller) to dedupe — emits {@code (vehicleId, firstSeenTimestamp)} the
 * first time each vehicle is seen and ignores subsequent events.
 *
 * <p>Same semantic as the MobilityFlink {@code Q1ContinuousFunction}; the
 * differences are purely in the runtime API (Kafka Streams Processor vs
 * Flink {@code KeyedProcessFunction}).
 */
public class Q1ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, Long> {

    private static final Logger LOG = LoggerFactory.getLogger(Q1ContinuousProcessor.class);

    private final String storeName;
    private KeyValueStore<Integer, Boolean> seen;
    private ProcessorContext<Integer, Long> ctx;

    public Q1ContinuousProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Integer, Long> context) {
        this.ctx = context;
        this.seen = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        Integer vehicleId = record.key();
        BerlinMODTrip trip = record.value();
        if (vehicleId == null || trip == null || vehicleId == -1) return;

        Boolean alreadySeen = seen.get(vehicleId);
        if (alreadySeen == null || !alreadySeen) {
            seen.put(vehicleId, true);
            ctx.forward(new Record<>(vehicleId, trip.getTimestamp(), trip.getTimestamp()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Q1-continuous first-sighting: vehicle={} t={}", vehicleId, trip.getTimestamp());
            }
        }
    }
}
