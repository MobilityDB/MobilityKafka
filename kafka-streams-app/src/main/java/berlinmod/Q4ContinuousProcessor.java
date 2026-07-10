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
 * BerlinMOD-Q4 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Which vehicles entered region R (transition outside → inside)?"</i>
 *
 * <p>Keyed by vehicleId. Per-vehicle state tracks the last seen
 * inside-or-outside flag for R; on each event, detect outside → inside
 * transition and emit {@code (vehicleId, entryTime)}.
 *
 * <p>Predicate: pure-Java axis-aligned point-in-box. The rectangular region
 * is degenerate as a geographic predicate (no projection needed); a generic
 * polygon-R variant would route through {@link MEOSBridge} for MEOS
 * {@code eintersects_tgeo_geo}.
 */
public class Q4ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, Long> {

    private final String storeName;
    private final double xmin, ymin, xmax, ymax;
    private KeyValueStore<Integer, Boolean> wasInside;
    private ProcessorContext<Integer, Long> ctx;

    public Q4ContinuousProcessor(String storeName, double xmin, double ymin, double xmax, double ymax) {
        this.storeName = storeName;
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    @Override
    public void init(ProcessorContext<Integer, Long> context) {
        this.ctx = context;
        this.wasInside = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        boolean isInside = inBox(trip.getLon(), trip.getLat());
        Boolean prev = wasInside.get(trip.getVehicleId());
        boolean prevInside = prev != null && prev;
        if (isInside && !prevInside) {
            ctx.forward(new Record<>(trip.getVehicleId(), trip.getTimestamp(), trip.getTimestamp()));
        }
        wasInside.put(trip.getVehicleId(), isInside);
    }

    private boolean inBox(double lon, double lat) {
        return MEOSBridge.intersectsBox(lon, lat, xmin, ymin, xmax, ymax);
    }
}
