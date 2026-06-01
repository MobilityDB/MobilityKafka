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
 * BerlinMOD-Q5 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, which pairs of vehicles met near P
 * (using each vehicle's last-seen-in-window position)?"</i>
 *
 * <p>State value encodes per-window per-vehicle last position as
 * {@code "vid:lon,lat|vid:lon,lat|..."}. On the STREAM_TIME punctuator
 * for each closed window, filter to near-P, enumerate sorted pairs,
 * forward meeting pairs.
 */
public class Q5WindowedProcessor implements Processor<Integer, BerlinMODTrip, String, Double> {

    private final String storeName;
    private final double pLon, pLat, dPMetres, dMeetMetres;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState;
    private ProcessorContext<String, Double> ctx;

    public Q5WindowedProcessor(String storeName, double pLon, double pLat,
                                double dPMetres, double dMeetMetres, long windowSizeMs) {
        this.storeName = storeName;
        this.pLon = pLon;
        this.pLat = pLat;
        this.dPMetres = dPMetres;
        this.dMeetMetres = dMeetMetres;
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void init(ProcessorContext<String, Double> context) {
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
        StringBuilder rebuilt = new StringBuilder();
        boolean replaced = false;
        if (s != null && !s.isEmpty()) {
            for (String chunk : s.split("\\|")) {
                int colon = chunk.indexOf(':');
                int vid = Integer.parseInt(chunk.substring(0, colon));
                if (vid == trip.getVehicleId()) {
                    if (rebuilt.length() > 0) rebuilt.append("|");
                    rebuilt.append(vid).append(":").append(trip.getLon()).append(",").append(trip.getLat());
                    replaced = true;
                } else {
                    if (rebuilt.length() > 0) rebuilt.append("|");
                    rebuilt.append(chunk);
                }
            }
        }
        if (!replaced) {
            if (rebuilt.length() > 0) rebuilt.append("|");
            rebuilt.append(trip.getVehicleId()).append(":").append(trip.getLon()).append(",").append(trip.getLat());
        }
        winState.put(winStart, rebuilt.toString());
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
            // Parse positions, filter near-P
            List<int[]> nearIds = new ArrayList<>();
            List<double[]> positions = new ArrayList<>();
            for (String chunk : closedStates.get(i).split("\\|")) {
                int colon = chunk.indexOf(':');
                if (colon < 0) continue;
                int vid = Integer.parseInt(chunk.substring(0, colon));
                String[] ll = chunk.substring(colon + 1).split(",", 2);
                double lon = Double.parseDouble(ll[0]);
                double lat = Double.parseDouble(ll[1]);
                if (MEOSBridge.dwithinMetres(lon, lat, pLon, pLat, dPMetres)) {
                    nearIds.add(new int[]{vid});
                    positions.add(new double[]{lon, lat});
                }
            }
            int n = nearIds.size();
            for (int a = 0; a < n - 1; a++) {
                for (int b = a + 1; b < n; b++) {
                    if (nearIds.get(a)[0] > nearIds.get(b)[0]) {
                        int[] ti = nearIds.get(a); nearIds.set(a, nearIds.get(b)); nearIds.set(b, ti);
                        double[] tp = positions.get(a); positions.set(a, positions.get(b)); positions.set(b, tp);
                    }
                }
            }
            for (int a = 0; a < n; a++) {
                for (int b = a + 1; b < n; b++) {
                    double d = MEOSBridge.distanceMetres(
                            positions.get(a)[0], positions.get(a)[1],
                            positions.get(b)[0], positions.get(b)[1]);
                    if (d <= dMeetMetres) {
                        String pairKey = nearIds.get(a)[0] + "_" + nearIds.get(b)[0] + "@win" + winStart;
                        ctx.forward(new Record<>(pairKey, d, winStart + windowSizeMs - 1));
                    }
                }
            }
            winState.delete(winStart);
        }
    }
}
