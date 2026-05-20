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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * BerlinMOD-Q8 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, how many distinct vehicles were
 * within {@code d} metres of the road segment at any time during the
 * window?"</i>
 *
 * <p>Same shape as {@link Q3WindowedProcessor} with the segment-distance
 * predicate substituted for the point-radius one.
 */
public class Q8WindowedProcessor implements Processor<Integer, BerlinMODTrip, Long, Long> {

    private final String storeName;
    private final double s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState;
    private ProcessorContext<Long, Long> ctx;

    public Q8WindowedProcessor(String storeName, double s1Lon, double s1Lat,
                                double s2Lon, double s2Lat, double radiusMetres,
                                long windowSizeMs) {
        this.storeName = storeName;
        this.s1Lon = s1Lon;
        this.s1Lat = s1Lat;
        this.s2Lon = s2Lon;
        this.s2Lat = s2Lat;
        this.radiusMetres = radiusMetres;
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void init(ProcessorContext<Long, Long> context) {
        this.ctx = context;
        this.winState = context.getStateStore(storeName);
        context.schedule(Duration.ofMillis(windowSizeMs),
                         PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        BerlinMODTrip trip = record.value();
        if (trip == null || trip.getVehicleId() == -1) return;
        if (!SegmentDistance.withinMetres(trip.getLon(), trip.getLat(),
                                          s1Lon, s1Lat, s2Lon, s2Lat,
                                          radiusMetres)) {
            return;
        }
        long winStart = (trip.getTimestamp() / windowSizeMs) * windowSizeMs;
        String prior = winState.get(winStart);
        String sv = Integer.toString(trip.getVehicleId());
        if (prior == null) {
            winState.put(winStart, sv);
        } else if (!contains(prior, sv)) {
            winState.put(winStart, prior + "," + sv);
        }
    }

    private void punctuate(long currentStreamTime) {
        List<Long> toEmit = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        try (KeyValueIterator<Long, String> it = winState.all()) {
            while (it.hasNext()) {
                KeyValue<Long, String> kv = it.next();
                if (kv.key + windowSizeMs <= currentStreamTime) {
                    toEmit.add(kv.key);
                    Set<String> distinct = new HashSet<>();
                    for (String s : kv.value.split(",")) distinct.add(s);
                    counts.add((long) distinct.size());
                }
            }
        }
        Integer[] idx = new Integer[toEmit.size()];
        for (int i = 0; i < idx.length; i++) idx[i] = i;
        java.util.Arrays.sort(idx, Comparator.comparingLong(toEmit::get));
        for (Integer i : idx) {
            long winStart = toEmit.get(i);
            ctx.forward(new Record<>(winStart, counts.get(i), winStart + windowSizeMs - 1));
            winState.delete(winStart);
        }
    }

    private static boolean contains(String csv, String id) {
        for (String s : csv.split(",")) if (s.equals(id)) return true;
        return false;
    }
}
