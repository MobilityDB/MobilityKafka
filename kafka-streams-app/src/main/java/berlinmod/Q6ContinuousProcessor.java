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
 * and the running total in metres. On each event, accumulate the MEOS geog_distance
 * delta and emit the cumulative total.
 *
 * <p>State value uses a small string encoding "lon,lat,total" since the
 * scaffold avoids declaring a dedicated tuple SerDe; the encoding is
 * private to this processor.
 *
 * <p>Cumulative distance: per consecutive position-pair via
 * {@link MEOSBridge#distanceMetres}. The future "full" path uses MEOS'
 * {@code tpoint_length} over an aggregated trajectory; the per-event
 * cumulative form is the same numeric quantity either way.
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
            total = prevTotal + MEOSBridge.distanceMetres(lastLon, lastLat, trip.getLon(), trip.getLat());
        }
        state.put(trip.getVehicleId(), trip.getLon() + "," + trip.getLat() + "," + total);
        ctx.forward(new Record<>(trip.getVehicleId(), total, trip.getTimestamp()));
    }
}
