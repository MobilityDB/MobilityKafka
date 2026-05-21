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
 * BerlinMOD-Q7 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, for each (vehicle, POI), the first
 * event in the window where the vehicle is inside the POI."</i> Intra-
 * window scoping (no cross-window first-passage state).
 *
 * <p>State encodes per-window the recorded (vehicle, POI, time) triples
 * as {@code "vid:poiId:t,vid:poiId:t,..."}. On each event scan POIs; if
 * vehicle inside and this (vehicle, POI) not yet recorded for this
 * window, append. Punctuator emits each recorded triple for closed
 * windows.
 */
public class Q7WindowedProcessor implements Processor<Integer, BerlinMODTrip, Long, String> {

    private final String storeName;
    private final List<PointOfInterest> pois;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState;
    private ProcessorContext<Long, String> ctx;

    public Q7WindowedProcessor(String storeName, List<PointOfInterest> pois, long windowSizeMs) {
        this.storeName = storeName;
        this.pois = pois;
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
        if (s == null) s = "";
        StringBuilder appended = new StringBuilder(s);
        for (PointOfInterest poi : pois) {
            String marker = trip.getVehicleId() + ":" + poi.id + ":";
            if (s.contains(marker)) continue;
            if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(), poi.lon, poi.lat, poi.radiusMetres)) {
                if (appended.length() > 0) appended.append(",");
                appended.append(trip.getVehicleId()).append(":").append(poi.id).append(":").append(trip.getTimestamp());
            }
        }
        if (appended.length() > s.length()) {
            winState.put(winStart, appended.toString());
        } else if (s.isEmpty()) {
            winState.put(winStart, "");
        }
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
            if (s != null && !s.isEmpty()) {
                List<int[]> vps = new ArrayList<>();
                List<long[]> times = new ArrayList<>();
                for (String chunk : s.split(",")) {
                    String[] f = chunk.split(":", 3);
                    vps.add(new int[]{Integer.parseInt(f[0]), Integer.parseInt(f[1])});
                    times.add(new long[]{Long.parseLong(f[2])});
                }
                Integer[] sortedIdx = new Integer[vps.size()];
                for (int k = 0; k < sortedIdx.length; k++) sortedIdx[k] = k;
                java.util.Arrays.sort(sortedIdx, Comparator
                        .comparingInt((Integer k) -> vps.get(k)[0])
                        .thenComparingInt(k -> vps.get(k)[1]));
                for (Integer k : sortedIdx) {
                    int[] vp = vps.get(k);
                    ctx.forward(new Record<>(winStart,
                            vp[0] + ":" + vp[1] + ":" + times.get(k)[0],
                            winStart + windowSizeMs - 1));
                }
            }
            winState.delete(winStart);
        }
    }
}
