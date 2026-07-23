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
