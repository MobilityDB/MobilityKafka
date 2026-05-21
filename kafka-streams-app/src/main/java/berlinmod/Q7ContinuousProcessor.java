package berlinmod;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

/**
 * BerlinMOD-Q7 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"For each (vehicle, POI) pair, when did the vehicle first come
 * within the POI's radius?"</i>
 *
 * <p>Keyed by vehicleId. State is a {@link KeyValueStore} whose composite
 * key is the POI id, scoped per-vehicle by Kafka Streams' implicit
 * per-key state partitioning. On each event, scan the POI list; for each
 * POI not yet recorded as passed and within radius, record the time and
 * emit {@code (poiId, firstPassageTime)} (the key carries the vehicleId
 * implicitly via the upstream keying).
 */
public class Q7ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, Long> {

    private final String storeName;
    private final List<PointOfInterest> pois;
    private KeyValueStore<Integer, Long> firstPassed; // poiId -> firstPassageTime, per-vehicle by keying
    private ProcessorContext<Integer, Long> ctx;

    public Q7ContinuousProcessor(String storeName, List<PointOfInterest> pois) {
        this.storeName = storeName;
        this.pois = pois;
    }

    @Override
    public void init(ProcessorContext<Integer, Long> context) {
        this.ctx = context;
        this.firstPassed = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        Integer vehicleId = record.key();
        for (PointOfInterest poi : pois) {
            int compositeKey = compositeKey(vehicleId, poi.id);
            if (firstPassed.get(compositeKey) != null) continue;
            if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(), poi.lon, poi.lat, poi.radiusMetres)) {
                firstPassed.put(compositeKey, trip.getTimestamp());
                ctx.forward(new Record<>(poi.id, trip.getTimestamp(), trip.getTimestamp()));
            }
        }
    }

    /** Pack (vehicleId, poiId) into a single Integer for the store key. */
    private static int compositeKey(int vehicleId, int poiId) {
        return (vehicleId * 1000) + poiId;
    }
}
