package berlinmod;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * BerlinMOD-Q8 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Is this vehicle currently within {@code d} metres of the road
 * segment?"</i>
 *
 * <p>Stateless per-event predicate using planar point-to-segment distance.
 * Same shape as {@link Q3ContinuousProcessor} but with a segment-distance
 * predicate instead of a point-radius one.
 */
public class Q8ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, Boolean> {

    private final double s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres;
    private ProcessorContext<Integer, Boolean> ctx;

    public Q8ContinuousProcessor(double s1Lon, double s1Lat, double s2Lon, double s2Lat, double radiusMetres) {
        this.s1Lon = s1Lon;
        this.s1Lat = s1Lat;
        this.s2Lon = s2Lon;
        this.s2Lat = s2Lat;
        this.radiusMetres = radiusMetres;
    }

    @Override
    public void init(ProcessorContext<Integer, Boolean> context) {
        this.ctx = context;
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        boolean near = MEOSBridge.dwithinSegmentMetres(
                trip.getLon(), trip.getLat(),
                s1Lon, s1Lat, s2Lon, s2Lat,
                radiusMetres);
        ctx.forward(new Record<>(trip.getVehicleId(), near, trip.getTimestamp()));
    }
}
