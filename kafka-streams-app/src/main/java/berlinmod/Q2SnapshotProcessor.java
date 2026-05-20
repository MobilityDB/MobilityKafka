package berlinmod;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

/**
 * BerlinMOD-Q2 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, where is vehicle X?"</i> using X's most-recent-known
 * position on or before T.
 *
 * <p>Single-key state (key=0) value "lon,lat,t" updated only when an event
 * arrives for the queried {@code targetVehicleId}. STREAM_TIME punctuator
 * every {@code snapshotTickMillis} emits {@code (currentTick, lon, lat,
 * lastEventT)} when state is set.
 */
public class Q2SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final int targetVehicleId;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, String> state;
    private ProcessorContext<Long, String> ctx;

    public Q2SnapshotProcessor(String storeName, int targetVehicleId, long snapshotTickMillis) {
        this.storeName = storeName;
        this.targetVehicleId = targetVehicleId;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<Long, String> context) {
        this.ctx = context;
        this.state = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(snapshotTickMillis),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        if (trip.getVehicleId() == targetVehicleId) {
            state.put(0, trip.getLon() + "," + trip.getLat() + "," + trip.getTimestamp());
        }
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        String v = state.get(0);
        if (v != null) {
            ctx.forward(new Record<>(tick, v, tick));
        }
    }
}
