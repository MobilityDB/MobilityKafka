package berlinmod;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * BerlinMOD-Q5 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Which pairs of vehicles are currently meeting near point P?"</i>
 *
 * <p>Caller should key the input stream by a constant so the shared
 * cross-vehicle last-known state lives in one subtask. Per-event:
 * update last-known of the event's vehicle, then enumerate all known
 * pairs and forward {@code (a, b, eventTime, distanceMetres)} encoded
 * as a string key + Double value for every currently-meeting pair
 * (a &lt; b for stable identity).
 *
 * <p>State value encoded as "lon,lat" string; the encoding is private
 * to this processor (avoids declaring a tuple SerDe for the scaffold).
 */
public class Q5ContinuousProcessor implements Processor<Integer, BerlinMODTrip, String, Double> {

    private final String storeName;
    private final double pLon, pLat, dPMetres, dMeetMetres;
    private KeyValueStore<Integer, String> lastPos;
    private ProcessorContext<String, Double> ctx;

    public Q5ContinuousProcessor(String storeName, double pLon, double pLat,
                                  double dPMetres, double dMeetMetres) {
        this.storeName = storeName;
        this.pLon = pLon;
        this.pLat = pLat;
        this.dPMetres = dPMetres;
        this.dMeetMetres = dMeetMetres;
    }

    @Override
    public void init(ProcessorContext<String, Double> context) {
        this.ctx = context;
        this.lastPos = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        lastPos.put(trip.getVehicleId(), trip.getLon() + "," + trip.getLat());

        // Snapshot near-P vehicles (sorted by id for stable pair iteration)
        List<int[]> ids = new ArrayList<>();
        List<double[]> positions = new ArrayList<>();
        try (KeyValueIterator<Integer, String> it = lastPos.all()) {
            while (it.hasNext()) {
                KeyValue<Integer, String> kv = it.next();
                String[] ll = kv.value.split(",", 2);
                double lon = Double.parseDouble(ll[0]);
                double lat = Double.parseDouble(ll[1]);
                if (Haversine.withinMetres(lon, lat, pLon, pLat, dPMetres)) {
                    ids.add(new int[]{kv.key});
                    positions.add(new double[]{lon, lat});
                }
            }
        }
        // Sort by id for stable output (small N — bubble is fine)
        int n = ids.size();
        for (int i = 0; i < n - 1; i++) {
            for (int j = i + 1; j < n; j++) {
                if (ids.get(i)[0] > ids.get(j)[0]) {
                    int[] ti = ids.get(i); ids.set(i, ids.get(j)); ids.set(j, ti);
                    double[] tp = positions.get(i); positions.set(i, positions.get(j)); positions.set(j, tp);
                }
            }
        }
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                double d = Haversine.distanceMetres(
                        positions.get(i)[0], positions.get(i)[1],
                        positions.get(j)[0], positions.get(j)[1]);
                if (d <= dMeetMetres) {
                    String pairKey = ids.get(i)[0] + "_" + ids.get(j)[0] + "@" + trip.getTimestamp();
                    ctx.forward(new Record<>(pairKey, d, trip.getTimestamp()));
                }
            }
        }
    }
}
