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
 * BerlinMOD-Q1 — <b>snapshot form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"At time T, which vehicles have appeared in the stream up to T?"</i>
 *
 * <p>The parity-oracle form: streaming output at watermark T equals the
 * batch BerlinMOD-Q1 result on data up to T.
 *
 * <p>Caller keys the input by a constant so the shared cross-vehicle "seen"
 * map lives in a single subtask. On each event (sentinel vehicleId == -1
 * is ignored), record vehicleId → firstSeenTime if not already present.
 * On each STREAM_TIME punctuator fire (every {@code snapshotTickMillis}),
 * walk the map and forward {@code (currentStreamTime, vehicleId)} per
 * recorded vehicle, sorted by vehicleId for deterministic output.
 */
public class Q1SnapshotProcessor implements Processor<Integer, BerlinMODTrip, Long, Integer> {

    private final String storeName;
    private final long snapshotTickMillis;
    private KeyValueStore<Integer, Long> seen;
    private ProcessorContext<Long, Integer> ctx;

    public Q1SnapshotProcessor(String storeName, long snapshotTickMillis) {
        this.storeName = storeName;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void init(ProcessorContext<Long, Integer> context) {
        this.ctx = context;
        this.seen = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(snapshotTickMillis),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        if (seen.get(trip.getVehicleId()) == null) {
            seen.put(trip.getVehicleId(), trip.getTimestamp());
        }
    }

    private void punctuate(long currentStreamTime) {
        long tick = (currentStreamTime / snapshotTickMillis) * snapshotTickMillis;
        List<Integer> ids = new ArrayList<>();
        try (KeyValueIterator<Integer, Long> it = seen.all()) {
            while (it.hasNext()) {
                KeyValue<Integer, Long> kv = it.next();
                ids.add(kv.key);
            }
        }
        ids.sort(Comparator.naturalOrder());
        for (Integer vehicleId : ids) {
            ctx.forward(new Record<>(tick, vehicleId, tick));
        }
    }
}
