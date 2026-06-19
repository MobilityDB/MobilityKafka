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
 * BerlinMOD-Q7 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, for each (vehicle, POI), the first time the vehicle
 * came within the POI's radius on or before T."</i>
 *
 * <p>Caller keys the input by a constant. Store key is the composite
 * {@code vehicleId * 1000 + poiId} integer with first-passage timestamp
 * as value. Per event: detect any (vehicle, POI) first-passages. Per
 * STREAM_TIME punctuator fire: walk store, emit each
 * {@code (currentTick, "vid:poiId:firstPassageTime")} for entries
 * with firstPassageTime ≤ currentTick.
 */
public class Q7SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final List<PointOfInterest> pois;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, Long> firstPassed; // compositeKey -> firstPassageTime
    private ProcessorContext<Long, String> ctx;

    public Q7SnapshotProcessor(String storeName, List<PointOfInterest> pois, long snapshotTickMillis) {
        this.storeName = storeName;
        this.pois = pois;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<Long, String> context) {
        this.ctx = context;
        this.firstPassed = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(snapshotTickMillis),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        for (PointOfInterest poi : pois) {
            int composite = trip.getVehicleId() * 1000 + poi.id;
            if (firstPassed.get(composite) != null) continue;
            if (Haversine.withinMetres(trip.getLon(), trip.getLat(), poi.lon, poi.lat, poi.radiusMetres)) {
                firstPassed.put(composite, trip.getTimestamp());
            }
        }
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        List<int[]> rows = new ArrayList<>(); // {vehicleId, poiId, firstPassageTime as int-of-long? — keep long via array of longs}
        // separate list for longs since arrays of ints lose precision
        List<Long> firstPassages = new ArrayList<>();
        try (KeyValueIterator<Integer, Long> it = firstPassed.all()) {
            while (it.hasNext()) {
                KeyValue<Integer, Long> kv = it.next();
                if (kv.value <= tick) {
                    int vid = kv.key / 1000;
                    int poiId = kv.key % 1000;
                    rows.add(new int[]{vid, poiId});
                    firstPassages.add(kv.value);
                }
            }
        }
        // Sort by (vehicleId, poiId) for deterministic output via parallel sort of two lists
        Integer[] idx = new Integer[rows.size()];
        for (int i = 0; i < idx.length; i++) idx[i] = i;
        java.util.Arrays.sort(idx, Comparator
                .comparingInt((Integer i) -> rows.get(i)[0])
                .thenComparingInt(i -> rows.get(i)[1]));
        for (Integer i : idx) {
            int[] r = rows.get(i);
            ctx.forward(new Record<>(tick, r[0] + ":" + r[1] + ":" + firstPassages.get(i), tick));
        }
    }
}
