package berlinmod;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * BerlinMOD-Q6 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"What is each vehicle's cumulative distance travelled so far?"</i>
 *
 * <p>Keyed by vehicleId. Per-vehicle state holds the last-known (lon, lat)
 * and the running total in metres. On each event, accumulate the Haversine
 * delta and emit the cumulative total.
 *
 * <p>State value uses a small string encoding "lon,lat,total" since the
 * scaffold avoids declaring a dedicated tuple SerDe; the encoding is
 * private to this processor.
 *
 * <p>TODO(meos): replace with the MEOS trajectory {@code length} call via
 * the JMEOS bridge.
 */
public class Q6ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, Double> {

    private final String storeName;
    private KeyValueStore<Integer, String> state; // "lastLon,lastLat,total"
    private ProcessorContext<Integer, Double> ctx;

    public Q6ContinuousProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Integer, Double> context) {
        this.ctx = context;
        this.state = context.getStateStore(storeName);
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
        ctx.forward(new Record<>(trip.getVehicleId(), total, trip.getTimestamp()));
    }
}
