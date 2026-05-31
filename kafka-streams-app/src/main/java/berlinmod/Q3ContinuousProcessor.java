package berlinmod;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * BerlinMOD-Q3 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Is this vehicle currently within {@code d} metres of point P?"</i>
 *
 * <p>Stateless per-event predicate: forward {@code (vehicleId,
 * eventTime, near)} per incoming GPS event. Same predicate semantics
 * as MobilityFlink's {@code Q3ContinuousFunction}.
 *
 * <p>Predicate: {@link MEOS {@code edwithin_tgeo_geo} over WGS84 geographies.
 */
public class Q3ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, Boolean> {

    private final double pLon;
    private final double pLat;
    private final double radiusMetres;
    private ProcessorContext<Integer, Boolean> ctx;

    public Q3ContinuousProcessor(double pLon, double pLat, double radiusMetres) {
        this.pLon = pLon;
        this.pLat = pLat;
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
        boolean near = MEOSBridge.dwithinMetres(
                trip.getLon(), trip.getLat(), pLon, pLat, radiusMetres);
        ctx.forward(new Record<>(trip.getVehicleId(), near, trip.getTimestamp()));
    }
}
