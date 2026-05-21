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
 * BerlinMOD-Q3 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, which vehicles are within {@code d} metres of point P?"</i>
 * using each vehicle's most-recent-known position on or before T.
 *
 * <p>Caller keys the input by a constant so the shared cross-vehicle
 * last-known store lives in one subtask. Per event: update last-known.
 * Per STREAM_TIME punctuator fire: iterate last-known, evaluate the
 * Haversine radius predicate, forward {@code (currentTick, vehicleId)}
 * for every near vehicle (sorted by vehicleId).
 */
public class Q3SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, Integer> {

    private final String storeName;
    private final double pLon, pLat, radiusMetres;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, String> lastPos; // vehicleId -> "lon,lat"
    private ProcessorContext<Long, Integer> ctx;

    public Q3SnapshotProcessor(String storeName, double pLon, double pLat,
                               double radiusMetres, long snapshotTickMillis) {
        this.storeName = storeName;
        this.pLon = pLon;
        this.pLat = pLat;
        this.radiusMetres = radiusMetres;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<Long, Integer> context) {
        this.ctx = context;
        this.lastPos = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(snapshotTickMillis),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        lastPos.put(trip.getVehicleId(), trip.getLon() + "," + trip.getLat());
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        List<Integer> nearIds = new ArrayList<>();
        try (KeyValueIterator<Integer, String> it = lastPos.all()) {
            while (it.hasNext()) {
                KeyValue<Integer, String> kv = it.next();
                String[] ll = kv.value.split(",", 2);
                double lon = Double.parseDouble(ll[0]);
                double lat = Double.parseDouble(ll[1]);
                if (MEOSBridge.dwithinMetres(lon, lat, pLon, pLat, radiusMetres)) {
                    nearIds.add(kv.key);
                }
            }
        }
        nearIds.sort(Comparator.naturalOrder());
        for (Integer vid : nearIds) {
            ctx.forward(new Record<>(tick, vid, tick));
        }
    }
}
