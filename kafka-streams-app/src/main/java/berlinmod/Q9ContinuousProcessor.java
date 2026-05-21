package berlinmod;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * BerlinMOD-Q9 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"What is the current distance between vehicles X and Y?"</i>
 *
 * <p>Caller should pre-filter the stream to {@code vehicleId ∈ {X, Y}} and
 * key by a constant so the shared X+Y state lives in a single subtask.
 * State encoded as a "xLon,xLat|yLon,yLat" string with NaN sentinels for
 * unseen slots; per-event update the X or Y slot then forward
 * {@code (eventTime, distanceMetres)} if both slots are known.
 */
public class Q9ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Long, Double> {

    private static final String UNSET = "NaN,NaN";

    private final String storeName;
    private final int xVehicleId;
    private final int yVehicleId;
    private KeyValueStore<Integer, String> state; // single-key (0) -> "xLon,xLat|yLon,yLat"
    private ProcessorContext<Long, Double> ctx;

    public Q9ContinuousProcessor(String storeName, int xVehicleId, int yVehicleId) {
        this.storeName = storeName;
        this.xVehicleId = xVehicleId;
        this.yVehicleId = yVehicleId;
    }

    @Override
    public void init(ProcessorContext<Long, Double> context) {
        this.ctx = context;
        this.state = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        String s = state.get(0);
        if (s == null) {
            s = UNSET + "|" + UNSET;
        }
        String[] parts = s.split("\\|", 2);
        String xSlot = parts[0];
        String ySlot = parts[1];
        if (trip.getVehicleId() == xVehicleId) {
            xSlot = trip.getLon() + "," + trip.getLat();
        } else if (trip.getVehicleId() == yVehicleId) {
            ySlot = trip.getLon() + "," + trip.getLat();
        } else {
            return;
        }
        state.put(0, xSlot + "|" + ySlot);
        if (!xSlot.startsWith("NaN") && !ySlot.startsWith("NaN")) {
            String[] x = xSlot.split(",", 2);
            String[] y = ySlot.split(",", 2);
            double d = MEOSBridge.distanceMetres(
                    Double.parseDouble(x[0]), Double.parseDouble(x[1]),
                    Double.parseDouble(y[0]), Double.parseDouble(y[1]));
            ctx.forward(new Record<>(trip.getTimestamp(), d, trip.getTimestamp()));
        }
    }
}
