package berlinmod;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

/**
 * BerlinMOD-Q9 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, what is the distance between vehicles X and Y (using
 * their most-recent-known positions on or before T)?"</i>
 *
 * <p>Single-key state holds the X+Y position pair. Per event for X or Y:
 * update the slot. Per STREAM_TIME punctuator fire: if both slots are
 * known, emit {@code (currentTick, distanceMetres)}.
 */
public class Q9SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, Double> {

    private static final String UNSET = "NaN,NaN";

    private final String storeName;
    private final int xVehicleId;
    private final int yVehicleId;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, String> state; // key=0 -> "xLon,xLat|yLon,yLat"
    private ProcessorContext<Long, Double> ctx;

    public Q9SnapshotProcessor(String storeName, int xVehicleId, int yVehicleId, long snapshotTickMillis) {
        this.storeName = storeName;
        this.xVehicleId = xVehicleId;
        this.yVehicleId = yVehicleId;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<Long, Double> context) {
        this.ctx = context;
        this.state = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(snapshotTickMillis),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        String s = state.get(0);
        if (s == null) s = UNSET + "|" + UNSET;
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
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        String s = state.get(0);
        if (s == null) return;
        String[] parts = s.split("\\|", 2);
        if (parts[0].startsWith("NaN") || parts[1].startsWith("NaN")) return;
        String[] x = parts[0].split(",", 2);
        String[] y = parts[1].split(",", 2);
        double d = Haversine.distanceMetres(
                Double.parseDouble(x[0]), Double.parseDouble(x[1]),
                Double.parseDouble(y[0]), Double.parseDouble(y[1]));
        ctx.forward(new Record<>(tick, d, tick));
    }
}
