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
 * BerlinMOD-Q1 — <b>windowed form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Per N-second tumbling window, how many distinct vehicles appeared
 * in the window?"</i>
 *
 * <p>State value is a comma-separated set of vehicleIds seen in each
 * window start. STREAM_TIME punctuator at {@code windowSizeMs} interval
 * emits closed windows and removes them from the store.
 */
public class Q1WindowedProcessor implements Processor<Integer, BerlinMODTrip, Long, Long> {

    private final String storeName;
    private final long windowSizeMs;
    private KeyValueStore<Long, String> winState; // winStart -> "vid1,vid2,..."
    private ProcessorContext<Long, Long> ctx;

    public Q1WindowedProcessor(String storeName, long windowSizeMs) {
        this.storeName = storeName;
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
        long winStart = (trip.getTimestamp() / windowSizeMs) * windowSizeMs;
        String prior = winState.get(winStart);
        String sv = Integer.toString(trip.getVehicleId());
        if (prior == null) {
            winState.put(winStart, sv);
        } else if (!containsId(prior, sv)) {
            winState.put(winStart, prior + "," + sv);
        }
    }

    private void punctuate(long currentStreamTime) {
        // Emit closed windows (winEnd <= currentStreamTime) and remove
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
        // Sort by winStart for deterministic order
        Integer[] idx = new Integer[toEmit.size()];
        for (int i = 0; i < idx.length; i++) idx[i] = i;
        java.util.Arrays.sort(idx, Comparator.comparingLong(toEmit::get));
        for (Integer i : idx) {
            long winStart = toEmit.get(i);
            ctx.forward(new Record<>(winStart, counts.get(i), winStart + windowSizeMs - 1));
            winState.delete(winStart);
        }
    }

    private boolean containsId(String csv, String id) {
        if (csv == null) return false;
        for (String s : csv.split(",")) {
            if (s.equals(id)) return true;
        }
        return false;
    }
}
