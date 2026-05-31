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
 * BerlinMOD-Q4 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, which vehicles entered region R during
 * the window?"</i> Intra-window scoping: a vehicle's first event in the
 * window with {@code inBox(...) == true} counts as an entry (no cross-window
 * memory of prior inside-state).
 *
 * <p>State value encodes the entries already recorded for the window plus
 * a "last seen inside" flag per vehicle: comma-separated list of
 * {@code "vid:wasInside:entryTime"} triples. Tracks per-(window, vehicle)
 * to detect intra-window outside→inside transitions.
 */
public class Q4WindowedProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final double xmin, ymin, xmax, ymax;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState; // winStart -> "vid:wasInside:entries|..."
    private ProcessorContext<Long, String> ctx;

    public Q4WindowedProcessor(String storeName, double xmin, double ymin, double xmax, double ymax,
                                long windowSizeMs) {
        this.storeName = storeName;
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
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
        boolean curr = MEOSBridge.intersectsBox(trip.getLon(), trip.getLat(), xmin, ymin, xmax, ymax);
        String s = winState.get(winStart);
        // Parse per-vehicle records separated by '|'
        StringBuilder rebuilt = new StringBuilder();
        boolean foundVehicle = false;
        if (s != null && !s.isEmpty()) {
            for (String chunk : s.split("\\|")) {
                String[] f = chunk.split(":", 3);
                int vid = Integer.parseInt(f[0]);
                boolean wasInside = Boolean.parseBoolean(f[1]);
                String entries = f.length > 2 ? f[2] : "";
                if (vid == trip.getVehicleId()) {
                    foundVehicle = true;
                    String newEntries = entries;
                    if (curr && !wasInside) {
                        newEntries = entries.isEmpty()
                                ? Long.toString(trip.getTimestamp())
                                : entries + "," + trip.getTimestamp();
                    }
                    if (rebuilt.length() > 0) rebuilt.append("|");
                    rebuilt.append(vid).append(":").append(curr).append(":").append(newEntries);
                } else {
                    if (rebuilt.length() > 0) rebuilt.append("|");
                    rebuilt.append(chunk);
                }
            }
        }
        if (!foundVehicle) {
            // First event for this vehicle in this window — intra-window scoping
            // treats first-seen-inside as an entry.
            String entries = curr ? Long.toString(trip.getTimestamp()) : "";
            if (rebuilt.length() > 0) rebuilt.append("|");
            rebuilt.append(trip.getVehicleId()).append(":").append(curr).append(":").append(entries);
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
            // Emit one "vid:entryTime" per recorded entry, sorted by (vid, entryTime)
            List<int[]> vidIdx = new ArrayList<>();
            List<long[]> times = new ArrayList<>();
            for (String chunk : closedStates.get(i).split("\\|")) {
                if (chunk.isEmpty()) continue;
                String[] f = chunk.split(":", 3);
                int vid = Integer.parseInt(f[0]);
                String entries = f.length > 2 ? f[2] : "";
                if (entries.isEmpty()) continue;
                for (String s : entries.split(",")) {
                    vidIdx.add(new int[]{vid});
                    times.add(new long[]{Long.parseLong(s)});
                }
            }
            // Stable sort by vid then time
            Integer[] sortedIdx = new Integer[vidIdx.size()];
            for (int k = 0; k < sortedIdx.length; k++) sortedIdx[k] = k;
            java.util.Arrays.sort(sortedIdx, Comparator
                    .comparingInt((Integer k) -> vidIdx.get(k)[0])
                    .thenComparingLong(k -> times.get(k)[0]));
            for (Integer k : sortedIdx) {
                ctx.forward(new Record<>(winStart,
                        vidIdx.get(k)[0] + ":" + times.get(k)[0],
                        winStart + windowSizeMs - 1));
            }
            winState.delete(winStart);
        }
    }
}
