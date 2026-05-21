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
 * BerlinMOD-Q6 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, per vehicle, distance travelled
 * during the window."</i>
 *
 * <p>State encodes per-window per-vehicle {@code "vid:lastLon,lastLat,total|..."}.
 * On each event, accumulate Haversine delta from the previous in-window
 * position. On punctuator: emit per-vehicle totals for closed windows.
 */
public class Q6WindowedProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState;
    private ProcessorContext<Long, String> ctx;

    public Q6WindowedProcessor(String storeName, long windowSizeMs) {
        this.storeName = storeName;
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
        long winStart = (trip.getTimestamp() / windowSizeMs) * windowSizeMs;
        String s = winState.get(winStart);
        StringBuilder rebuilt = new StringBuilder();
        boolean replaced = false;
        if (s != null && !s.isEmpty()) {
            for (String chunk : s.split("\\|")) {
                int colon = chunk.indexOf(':');
                int vid = Integer.parseInt(chunk.substring(0, colon));
                String body = chunk.substring(colon + 1);
                if (vid == trip.getVehicleId()) {
                    String[] f = body.split(",", 3);
                    double lastLon = Double.parseDouble(f[0]);
                    double lastLat = Double.parseDouble(f[1]);
                    double prevTotal = Double.parseDouble(f[2]);
                    double newTotal = prevTotal + MEOSBridge.distanceMetres(
                            lastLon, lastLat, trip.getLon(), trip.getLat());
                    if (rebuilt.length() > 0) rebuilt.append("|");
                    rebuilt.append(vid).append(":")
                           .append(trip.getLon()).append(",")
                           .append(trip.getLat()).append(",")
                           .append(newTotal);
                    replaced = true;
                } else {
                    if (rebuilt.length() > 0) rebuilt.append("|");
                    rebuilt.append(chunk);
                }
            }
        }
        if (!replaced) {
            if (rebuilt.length() > 0) rebuilt.append("|");
            rebuilt.append(trip.getVehicleId()).append(":")
                   .append(trip.getLon()).append(",")
                   .append(trip.getLat()).append(",0.0");
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
            List<int[]> ids = new ArrayList<>();
            List<double[]> totals = new ArrayList<>();
            for (String chunk : closedStates.get(i).split("\\|")) {
                int colon = chunk.indexOf(':');
                if (colon < 0) continue;
                int vid = Integer.parseInt(chunk.substring(0, colon));
                String[] f = chunk.substring(colon + 1).split(",", 3);
                double total = Double.parseDouble(f[2]);
                ids.add(new int[]{vid});
                totals.add(new double[]{total});
            }
            Integer[] sortedIdx = new Integer[ids.size()];
            for (int k = 0; k < sortedIdx.length; k++) sortedIdx[k] = k;
            java.util.Arrays.sort(sortedIdx, Comparator.comparingInt(k -> ids.get(k)[0]));
            for (Integer k : sortedIdx) {
                ctx.forward(new Record<>(winStart,
                        ids.get(k)[0] + ":" + totals.get(k)[0],
                        winStart + windowSizeMs - 1));
            }
            winState.delete(winStart);
        }
    }
}
