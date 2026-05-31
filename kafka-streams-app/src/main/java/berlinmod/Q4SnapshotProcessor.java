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
 * BerlinMOD-Q4 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, what is the list of (vehicleId, entryTime) pairs for all
 * vehicles that entered region R at or before T?"</i>
 *
 * <p>Caller keys the input by a constant so the shared cross-vehicle state
 * lives in one subtask. Per-vehicle two stores: {@code wasInside} (Boolean)
 * and {@code entries} (semicolon-separated entry times). Per event: detect
 * outside→inside transition and append the entry time. Per STREAM_TIME
 * punctuator fire: walk every vehicle, emit one
 * {@code (currentTick, vehicleId, entryTime)} per recorded entry time
 * ≤ currentTick.
 */
public class Q4SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String wasInsideStoreName;
    private final String entriesStoreName;
    private final double xmin, ymin, xmax, ymax;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, Boolean> wasInside;
    private KeyValueStore<Integer, String> entries; // vehicleId -> "t1;t2;..."
    private ProcessorContext<Long, String> ctx;

    public Q4SnapshotProcessor(String wasInsideStoreName, String entriesStoreName,
                                double xmin, double ymin, double xmax, double ymax,
                                long snapshotTickMillis) {
        this.wasInsideStoreName = wasInsideStoreName;
        this.entriesStoreName = entriesStoreName;
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<Long, String> context) {
        this.ctx = context;
        this.wasInside = context.getStateStore(wasInsideStoreName);
        this.entries = context.getStateStore(entriesStoreName);
        context.schedule(Duration.ofMillis(snapshotTickMillis),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        boolean curr = MEOSBridge.intersectsBox(trip.getLon(), trip.getLat(), xmin, ymin, xmax, ymax);
        Boolean prev = wasInside.get(trip.getVehicleId());
        boolean prevInside = prev != null && prev;
        if (curr && !prevInside) {
            String prior = entries.get(trip.getVehicleId());
            String updated = (prior == null || prior.isEmpty())
                    ? Long.toString(trip.getTimestamp())
                    : prior + ";" + trip.getTimestamp();
            entries.put(trip.getVehicleId(), updated);
        }
        wasInside.put(trip.getVehicleId(), curr);
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        List<Integer> ids = new ArrayList<>();
        try (KeyValueIterator<Integer, String> it = entries.all()) {
            while (it.hasNext()) {
                KeyValue<Integer, String> kv = it.next();
                if (kv.value != null && !kv.value.isEmpty()) ids.add(kv.key);
            }
        }
        ids.sort(Comparator.naturalOrder());
        for (Integer vid : ids) {
            String list = entries.get(vid);
            for (String s : list.split(";")) {
                long entryTime = Long.parseLong(s);
                if (entryTime <= tick) {
                    ctx.forward(new Record<>(tick, vid + "@" + entryTime, tick));
                }
            }
        }
    }
}
