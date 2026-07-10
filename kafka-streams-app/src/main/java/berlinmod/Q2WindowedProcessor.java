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
 * BerlinMOD-Q2 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, last-known (lon, lat) of vehicle X
 * seen in the window."</i>
 *
 * <p>State value "lon,lat,t" overwritten on every X event whose timestamp
 * falls in {@code winStart}. STREAM_TIME punctuator at {@code windowSizeMs}
 * emits closed windows.
 */
public class Q2WindowedProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final int targetVehicleId;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState;
    private ProcessorContext<Long, String> ctx;

    public Q2WindowedProcessor(String storeName, int targetVehicleId, long windowSizeMs) {
        this.storeName = storeName;
        this.targetVehicleId = targetVehicleId;
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void init(ProcessorContext<Long, String> context) {
        this.ctx = context;
        this.winState = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(windowSizeMs),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        if (trip.getVehicleId() != targetVehicleId) return;
        long winStart = (trip.getTimestamp() / windowSizeMs) * windowSizeMs;
        winState.put(winStart, trip.getLon() + "," + trip.getLat() + "," + trip.getTimestamp());
    }

    private void punctuate(long currentStreamTime) {
        List<Long> toEmit = new ArrayList<>();
        List<String> values = new ArrayList<>();
        try (KeyValueIterator<Long, String> it = winState.all()) {
            while (it.hasNext()) {
                KeyValue<Long, String> kv = it.next();
                if (kv.key + windowSizeMs <= currentStreamTime) {
                    toEmit.add(kv.key);
                    values.add(kv.value);
                }
            }
        }
        Integer[] idx = new Integer[toEmit.size()];
        for (int i = 0; i < idx.length; i++) idx[i] = i;
        java.util.Arrays.sort(idx, Comparator.comparingLong(toEmit::get));
        for (Integer i : idx) {
            long winStart = toEmit.get(i);
            ctx.forward(new Record<>(winStart, values.get(i), winStart + windowSizeMs - 1));
            winState.delete(winStart);
        }
    }
}
