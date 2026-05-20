package berlinmod;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * BerlinMOD-Q6 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, what is each vehicle's total distance travelled
 * up to T?"</i>
 *
 * <p>Caller keys the input by a constant. State value "lon,lat,total"
 * per vehicleId. Per event: accumulate Haversine delta. Per STREAM_TIME
 * punctuator fire: emit {@code (currentTick, vehicleId, total)} for
 * every vehicle (sorted by vehicleId), encoded as "vid:total".
 */
public class Q6SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, String> state;
    private ProcessorContext<Long, String> ctx;

    public Q6SnapshotProcessor(String storeName, long snapshotTickMillis) {
        this.storeName = storeName;
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
        String prev = state.get(trip.getVehicleId());
        double total;
        if (prev == null) {
            total = 0.0;
        } else {
            String[] parts = prev.split(",", 3);
            double lastLon = Double.parseDouble(parts[0]);
            double lastLat = Double.parseDouble(parts[1]);
            double prevTotal = Double.parseDouble(parts[2]);
            total = prevTotal + Haversine.distanceMetres(lastLon, lastLat, trip.getLon(), trip.getLat());
        }
        state.put(trip.getVehicleId(), trip.getLon() + "," + trip.getLat() + "," + total);
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        List<Integer> ids = new ArrayList<>();
        try (KeyValueIterator<Integer, String> it = state.all()) {
            while (it.hasNext()) {
                KeyValue<Integer, String> kv = it.next();
                ids.add(kv.key);
            }
        }
        ids.sort(Comparator.naturalOrder());
        for (Integer vid : ids) {
            String[] parts = state.get(vid).split(",", 3);
            double total = Double.parseDouble(parts[2]);
            ctx.forward(new Record<>(tick, vid + ":" + total, tick));
        }
    }
}
