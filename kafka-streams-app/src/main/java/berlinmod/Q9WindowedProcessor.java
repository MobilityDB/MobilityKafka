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
 * BerlinMOD-Q9 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, X-Y distance at window-end using
 * each vehicle's last position in the window."</i>
 *
 * <p>State value encodes per-window the latest X and Y positions seen
 * as {@code "xLon,xLat|yLon,yLat"} (with {@code NaN,NaN} for unseen).
 * Punctuator emits distance for each closed window where both X and Y
 * are known.
 */
public class Q9WindowedProcessor implements Processor<Integer, BerlinMODTrip, Long, Double> {

    private static final String UNSET = "NaN,NaN";

    private final String storeName;
    private final int xVehicleId, yVehicleId;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState;
    private ProcessorContext<Long, Double> ctx;

    public Q9WindowedProcessor(String storeName, int xVehicleId, int yVehicleId, long windowSizeMs) {
        this.storeName = storeName;
        this.xVehicleId = xVehicleId;
        this.yVehicleId = yVehicleId;
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void init(ProcessorContext<Long, Double> context) {
        this.ctx = context;
        this.winState = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(windowSizeMs),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        long winStart = (trip.getTimestamp() / windowSizeMs) * windowSizeMs;
        String s = winState.get(winStart);
        if (s == null) s = UNSET + "|" + UNSET;
        String[] parts = s.split("\\|", 2);
        String xSlot = parts[0];
        String ySlot = parts[1];
        if (trip.getVehicleId() == xVehicleId) {
            xSlot = trip.getLon() + "," + trip.getLat();
        } else if (trip.getVehicleId() == yVehicleId) {
            ySlot = trip.getLon() + "," + trip.getLat();
        } else {
            return;
        }
        winState.put(winStart, xSlot + "|" + ySlot);
    }

    private void punctuate(long currentStreamTime) {
        List<Long> closedStarts = new ArrayList<>();
        List<String> closedStates = new ArrayList<>();
        try (KeyValueIterator<Long, String> it = winState.all()) {
            while (it.hasNext()) {
                KeyValue<Long, String> kv = it.next();
                if (kv.key + windowSizeMs <= currentStreamTime) {
                    closedStarts.add(kv.key);
                    closedStates.add(kv.value);
                }
            }
        }
        Integer[] idx = new Integer[closedStarts.size()];
        for (int i = 0; i < idx.length; i++) idx[i] = i;
        java.util.Arrays.sort(idx, Comparator.comparingLong(closedStarts::get));
        for (Integer i : idx) {
            long winStart = closedStarts.get(i);
            String s = closedStates.get(i);
            String[] parts = s.split("\\|", 2);
            if (!parts[0].startsWith("NaN") && !parts[1].startsWith("NaN")) {
                String[] x = parts[0].split(",", 2);
                String[] y = parts[1].split(",", 2);
                double d = MEOSBridge.distanceMetres(
                        Double.parseDouble(x[0]), Double.parseDouble(x[1]),
                        Double.parseDouble(y[0]), Double.parseDouble(y[1]));
                ctx.forward(new Record<>(winStart, d, winStart + windowSizeMs - 1));
            }
            winState.delete(winStart);
        }
    }
}
