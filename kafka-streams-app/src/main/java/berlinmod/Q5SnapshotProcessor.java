/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

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
 * BerlinMOD-Q5 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, which pairs of vehicles are meeting near point P
 * (using each vehicle's most-recent-known position on or before T)?"</i>
 *
 * <p>Caller keys the input by a constant so the shared cross-vehicle
 * last-known store lives in one subtask. Per event: update last-known.
 * Per STREAM_TIME punctuator fire: snapshot the map, filter to near-P,
 * enumerate all pairs (a<b), emit {@code ("a_b@tick", distance)} for
 * every meeting pair.
 */
public class Q5SnapshotProcessor implements Processor<Integer, BerlinMODTrip, String, Double> {

    private final String storeName;
    private final double pLon, pLat, dPMetres, dMeetMetres;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, String> lastPos; // vehicleId -> "lon,lat"
    private ProcessorContext<String, Double> ctx;

    public Q5SnapshotProcessor(String storeName, double pLon, double pLat,
                               double dPMetres, double dMeetMetres,
                               long snapshotTickMillis) {
        this.storeName = storeName;
        this.pLon = pLon;
        this.pLat = pLat;
        this.dPMetres = dPMetres;
        this.dMeetMetres = dMeetMetres;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<String, Double> context) {
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
        List<int[]> ids = new ArrayList<>();
        List<double[]> positions = new ArrayList<>();
        try (KeyValueIterator<Integer, String> it = lastPos.all()) {
            while (it.hasNext()) {
                KeyValue<Integer, String> kv = it.next();
                String[] ll = kv.value.split(",", 2);
                double lon = Double.parseDouble(ll[0]);
                double lat = Double.parseDouble(ll[1]);
                if (MEOSBridge.dwithinMetres(lon, lat, pLon, pLat, dPMetres)) {
                    ids.add(new int[]{kv.key});
                    positions.add(new double[]{lon, lat});
                }
            }
        }
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
                double d = MEOSBridge.distanceMetres(
                        positions.get(i)[0], positions.get(i)[1],
                        positions.get(j)[0], positions.get(j)[1]);
                if (d <= dMeetMetres) {
                    String pairKey = ids.get(i)[0] + "_" + ids.get(j)[0] + "@" + tick;
                    ctx.forward(new Record<>(pairKey, d, tick));
                }
            }
        }
    }
}
