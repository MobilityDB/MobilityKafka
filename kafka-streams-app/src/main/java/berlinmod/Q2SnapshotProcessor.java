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

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

/**
 * BerlinMOD-Q2 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, where is vehicle X?"</i> using X's most-recent-known
 * position on or before T.
 *
 * <p>Single-key state (key=0) value "lon,lat,t" updated only when an event
 * arrives for the queried {@code targetVehicleId}. STREAM_TIME punctuator
 * every {@code snapshotTickMillis} emits {@code (currentTick, lon, lat,
 * lastEventT)} when state is set.
 */
public class Q2SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final int targetVehicleId;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, String> state;
    private ProcessorContext<Long, String> ctx;

    public Q2SnapshotProcessor(String storeName, int targetVehicleId, long snapshotTickMillis) {
        this.storeName = storeName;
        this.targetVehicleId = targetVehicleId;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<Long, String> context) {
        this.ctx = context;
        this.state = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(snapshotTickMillis),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        if (trip.getVehicleId() == targetVehicleId) {
            state.put(0, trip.getLon() + "," + trip.getLat() + "," + trip.getTimestamp());
        }
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        String v = state.get(0);
        if (v != null) {
            ctx.forward(new Record<>(tick, v, tick));
        }
    }
}
