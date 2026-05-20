package berlinmod;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.List;

/**
 * BerlinMOD-9 unified Kafka-Streams topology.
 *
 * <p>Reads a single source topic {@link #INPUT_TOPIC} and fans out into
 * per-{@code Q<N>}-form output topics. Each output topic carries one form
 * of one query.
 */
public final class BerlinMODTopology {

    public static final String INPUT_TOPIC = "berlinmod";
    public static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    public static final long WINDOW_SIZE_MILLIS = 10_000L;

    public static final String Q1_WINDOWED_OUTPUT = "berlinmod-q1-windowed";
    public static final String Q1_WIN_STORE = "q1-win-store";
    public static final String Q2_WINDOWED_OUTPUT = "berlinmod-q2-windowed";
    public static final String Q2_WIN_STORE = "q2-win-store";
    public static final String Q3_WINDOWED_OUTPUT = "berlinmod-q3-windowed";
    public static final String Q3_WIN_STORE = "q3-win-store";
    public static final String Q4_WINDOWED_OUTPUT = "berlinmod-q4-windowed";
    public static final String Q4_WIN_STORE = "q4-win-store";
    public static final String Q5_WINDOWED_OUTPUT = "berlinmod-q5-windowed";
    public static final String Q5_WIN_STORE = "q5-win-store";
    public static final String Q6_WINDOWED_OUTPUT = "berlinmod-q6-windowed";
    public static final String Q6_WIN_STORE = "q6-win-store";
    public static final String Q7_WINDOWED_OUTPUT = "berlinmod-q7-windowed";
    public static final String Q7_WIN_STORE = "q7-win-store";
    public static final String Q8_WINDOWED_OUTPUT = "berlinmod-q8-windowed";
    public static final String Q8_WIN_STORE = "q8-win-store";
    public static final String Q9_WINDOWED_OUTPUT = "berlinmod-q9-windowed";
    public static final String Q9_WIN_STORE = "q9-win-store";

    // ---------- Q1 ----------
    public static final String Q1_CONTINUOUS_OUTPUT = "berlinmod-q1-continuous";
    public static final String Q1_SEEN_STORE = "q1-seen-store";
    public static final String Q1_SNAPSHOT_OUTPUT = "berlinmod-q1-snapshot";
    public static final String Q1_SNAP_STORE = "q1-snap-store";

    // ---------- Q2 ----------
    public static final String Q2_CONTINUOUS_OUTPUT = "berlinmod-q2-continuous";
    public static final int Q2_TARGET_VEHICLE_ID = 2;
    public static final String Q2_SNAPSHOT_OUTPUT = "berlinmod-q2-snapshot";
    public static final String Q2_SNAP_STORE = "q2-snap-store";

    // ---------- Q3 ----------
    public static final String Q3_CONTINUOUS_OUTPUT = "berlinmod-q3-continuous";
    // Query params below are anchored to the canonical BerlinMOD sample's
    // per-vehicle geometry (vehicles 1-5, centroids 7-18 km apart); radii use
    // km-scale margins so the dwithin partition is identical under the
    // pure-Java Haversine and the MEOS geodetic engines.
    public static final double Q3_P_LON = 4.4322;   // near vehicle 1
    public static final double Q3_P_LAT = 50.7670;
    public static final double Q3_RADIUS_METRES = 5_000.0;
    public static final String Q3_SNAPSHOT_OUTPUT = "berlinmod-q3-snapshot";
    public static final String Q3_SNAP_STORE = "q3-snap-store";

    // ---------- Q4 ----------
    public static final String Q4_CONTINUOUS_OUTPUT = "berlinmod-q4-continuous";
    public static final String Q4_WAS_INSIDE_STORE = "q4-was-inside-store";
    public static final double Q4_XMIN = 4.40, Q4_YMIN = 50.74, Q4_XMAX = 4.47, Q4_YMAX = 50.86;
    public static final String Q4_SNAPSHOT_OUTPUT = "berlinmod-q4-snapshot";
    public static final String Q4_SNAP_WAS_INSIDE_STORE = "q4-snap-was-inside-store";
    public static final String Q4_SNAP_ENTRIES_STORE = "q4-snap-entries-store";

    // ---------- Q5 ----------
    public static final String Q5_CONTINUOUS_OUTPUT = "berlinmod-q5-continuous";
    public static final String Q5_LAST_POS_STORE = "q5-last-pos-store";
    public static final double Q5_P_LON = 4.3822;   // midpoint of vehicles 1 and 2
    public static final double Q5_P_LAT = 50.7683;
    public static final double Q5_D_P_METRES = 5_000.0;
    public static final double Q5_D_MEET_METRES = 8_000.0;
    public static final String Q5_SNAPSHOT_OUTPUT = "berlinmod-q5-snapshot";
    public static final String Q5_SNAP_STORE = "q5-snap-store";

    // ---------- Q6 ----------
    public static final String Q6_CONTINUOUS_OUTPUT = "berlinmod-q6-continuous";
    public static final String Q6_STATE_STORE = "q6-state-store";
    public static final String Q6_SNAPSHOT_OUTPUT = "berlinmod-q6-snapshot";
    public static final String Q6_SNAP_STORE = "q6-snap-store";

    // ---------- Q7 ----------
    public static final String Q7_CONTINUOUS_OUTPUT = "berlinmod-q7-continuous";
    public static final String Q7_FIRST_PASSED_STORE = "q7-first-passed-store";
    public static final List<PointOfInterest> Q7_POIS = Arrays.asList(
            new PointOfInterest(1, 4.3321, 50.7696, 2_000.0),   // near vehicle 2
            new PointOfInterest(2, 4.4571, 50.8515, 2_000.0),   // near vehicle 3
            new PointOfInterest(3, 4.4252, 50.9190, 2_000.0));  // near vehicle 5
    public static final String Q7_SNAPSHOT_OUTPUT = "berlinmod-q7-snapshot";
    public static final String Q7_SNAP_STORE = "q7-snap-store";

    // ---------- Q8 ----------
    public static final String Q8_CONTINUOUS_OUTPUT = "berlinmod-q8-continuous";
    public static final double Q8_S1_LON = 4.3321, Q8_S1_LAT = 50.7696;  // vehicle 2
    public static final double Q8_S2_LON = 4.3063, Q8_S2_LAT = 50.8825;  // vehicle 4
    public static final double Q8_RADIUS_METRES = 5_000.0;
    public static final String Q8_SNAPSHOT_OUTPUT = "berlinmod-q8-snapshot";
    public static final String Q8_SNAP_STORE = "q8-snap-store";

    // ---------- Q9 ----------
    public static final String Q9_CONTINUOUS_OUTPUT = "berlinmod-q9-continuous";
    public static final String Q9_STATE_STORE = "q9-state-store";
    public static final int Q9_X_VEHICLE_ID = 1;
    public static final int Q9_Y_VEHICLE_ID = 2;
    public static final String Q9_SNAPSHOT_OUTPUT = "berlinmod-q9-snapshot";
    public static final String Q9_SNAP_STORE = "q9-snap-store";

    private BerlinMODTopology() {}

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        BerlinMODTripSerde tripSerde = new BerlinMODTripSerde();

        // ---- continuous-form state stores ----
        addStore(builder, Q1_SEEN_STORE, Serdes.Integer(), Serdes.Boolean());
        addStore(builder, Q4_WAS_INSIDE_STORE, Serdes.Integer(), Serdes.Boolean());
        addStore(builder, Q5_LAST_POS_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q6_STATE_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q7_FIRST_PASSED_STORE, Serdes.Integer(), Serdes.Long());
        addStore(builder, Q9_STATE_STORE, Serdes.Integer(), Serdes.String());

        // ---- windowed-form state stores ----
        addStore(builder, Q1_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q2_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q3_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q4_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q5_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q6_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q7_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q8_WIN_STORE, Serdes.Long(), Serdes.String());
        addStore(builder, Q9_WIN_STORE, Serdes.Long(), Serdes.String());

        // ---- snapshot-form state stores (separate to avoid co-write conflicts with continuous) ----
        addStore(builder, Q1_SNAP_STORE, Serdes.Integer(), Serdes.Long());
        addStore(builder, Q2_SNAP_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q3_SNAP_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q4_SNAP_WAS_INSIDE_STORE, Serdes.Integer(), Serdes.Boolean());
        addStore(builder, Q4_SNAP_ENTRIES_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q5_SNAP_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q6_SNAP_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q7_SNAP_STORE, Serdes.Integer(), Serdes.Long());
        addStore(builder, Q8_SNAP_STORE, Serdes.Integer(), Serdes.String());
        addStore(builder, Q9_SNAP_STORE, Serdes.Integer(), Serdes.String());

        // ---- streams ----
        KStream<Integer, BerlinMODTrip> trips =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), tripSerde));

        // Re-keyed by constant for the shared-state snapshot/multi-vehicle processors
        KStream<Integer, BerlinMODTrip> tripsK0 = trips.selectKey((k, v) -> 0);

        // ====== continuous form ======
        trips.process(() -> new Q1ContinuousProcessor(Q1_SEEN_STORE), Q1_SEEN_STORE)
             .to(Q1_CONTINUOUS_OUTPUT, Produced.with(Serdes.Integer(), Serdes.Long()));

        trips.process(() -> new Q2ContinuousProcessor(Q2_TARGET_VEHICLE_ID))
             .to(Q2_CONTINUOUS_OUTPUT, Produced.with(Serdes.Integer(), tripSerde));

        trips.process(() -> new Q3ContinuousProcessor(Q3_P_LON, Q3_P_LAT, Q3_RADIUS_METRES))
             .to(Q3_CONTINUOUS_OUTPUT, Produced.with(Serdes.Integer(), Serdes.Boolean()));

        trips.process(() -> new Q4ContinuousProcessor(Q4_WAS_INSIDE_STORE, Q4_XMIN, Q4_YMIN, Q4_XMAX, Q4_YMAX),
                      Q4_WAS_INSIDE_STORE)
             .to(Q4_CONTINUOUS_OUTPUT, Produced.with(Serdes.Integer(), Serdes.Long()));

        tripsK0.process(() -> new Q5ContinuousProcessor(Q5_LAST_POS_STORE,
                                                         Q5_P_LON, Q5_P_LAT, Q5_D_P_METRES, Q5_D_MEET_METRES),
                        Q5_LAST_POS_STORE)
               .to(Q5_CONTINUOUS_OUTPUT, Produced.with(Serdes.String(), Serdes.Double()));

        trips.process(() -> new Q6ContinuousProcessor(Q6_STATE_STORE), Q6_STATE_STORE)
             .to(Q6_CONTINUOUS_OUTPUT, Produced.with(Serdes.Integer(), Serdes.Double()));

        trips.process(() -> new Q7ContinuousProcessor(Q7_FIRST_PASSED_STORE, Q7_POIS),
                      Q7_FIRST_PASSED_STORE)
             .to(Q7_CONTINUOUS_OUTPUT, Produced.with(Serdes.Integer(), Serdes.Long()));

        trips.process(() -> new Q8ContinuousProcessor(Q8_S1_LON, Q8_S1_LAT, Q8_S2_LON, Q8_S2_LAT, Q8_RADIUS_METRES))
             .to(Q8_CONTINUOUS_OUTPUT, Produced.with(Serdes.Integer(), Serdes.Boolean()));

        tripsK0.process(() -> new Q9ContinuousProcessor(Q9_STATE_STORE, Q9_X_VEHICLE_ID, Q9_Y_VEHICLE_ID),
                        Q9_STATE_STORE)
               .to(Q9_CONTINUOUS_OUTPUT, Produced.with(Serdes.Long(), Serdes.Double()));

        // ====== windowed form (distinct-count per tumbling window for Q1/Q3/Q8) ======
        tripsK0.process(() -> new Q1WindowedProcessor(Q1_WIN_STORE, WINDOW_SIZE_MILLIS), Q1_WIN_STORE)
               .to(Q1_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.Long()));

        tripsK0.process(() -> new Q3WindowedProcessor(Q3_WIN_STORE,
                                                       Q3_P_LON, Q3_P_LAT, Q3_RADIUS_METRES,
                                                       WINDOW_SIZE_MILLIS),
                        Q3_WIN_STORE)
               .to(Q3_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.Long()));

        tripsK0.process(() -> new Q8WindowedProcessor(Q8_WIN_STORE,
                                                       Q8_S1_LON, Q8_S1_LAT, Q8_S2_LON, Q8_S2_LAT,
                                                       Q8_RADIUS_METRES, WINDOW_SIZE_MILLIS),
                        Q8_WIN_STORE)
               .to(Q8_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.Long()));

        tripsK0.process(() -> new Q2WindowedProcessor(Q2_WIN_STORE, Q2_TARGET_VEHICLE_ID, WINDOW_SIZE_MILLIS),
                        Q2_WIN_STORE)
               .to(Q2_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q4WindowedProcessor(Q4_WIN_STORE,
                                                       Q4_XMIN, Q4_YMIN, Q4_XMAX, Q4_YMAX,
                                                       WINDOW_SIZE_MILLIS),
                        Q4_WIN_STORE)
               .to(Q4_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q5WindowedProcessor(Q5_WIN_STORE,
                                                       Q5_P_LON, Q5_P_LAT, Q5_D_P_METRES, Q5_D_MEET_METRES,
                                                       WINDOW_SIZE_MILLIS),
                        Q5_WIN_STORE)
               .to(Q5_WINDOWED_OUTPUT, Produced.with(Serdes.String(), Serdes.Double()));

        tripsK0.process(() -> new Q6WindowedProcessor(Q6_WIN_STORE, WINDOW_SIZE_MILLIS), Q6_WIN_STORE)
               .to(Q6_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q7WindowedProcessor(Q7_WIN_STORE, Q7_POIS, WINDOW_SIZE_MILLIS), Q7_WIN_STORE)
               .to(Q7_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q9WindowedProcessor(Q9_WIN_STORE,
                                                       Q9_X_VEHICLE_ID, Q9_Y_VEHICLE_ID,
                                                       WINDOW_SIZE_MILLIS),
                        Q9_WIN_STORE)
               .to(Q9_WINDOWED_OUTPUT, Produced.with(Serdes.Long(), Serdes.Double()));

        // ====== snapshot form (all via constant key, with STREAM_TIME punctuators) ======
        tripsK0.process(() -> new Q1SnapshotProcessor(Q1_SNAP_STORE, SNAPSHOT_TICK_MILLIS), Q1_SNAP_STORE)
               .to(Q1_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.Integer()));

        tripsK0.process(() -> new Q2SnapshotProcessor(Q2_SNAP_STORE, Q2_TARGET_VEHICLE_ID, SNAPSHOT_TICK_MILLIS),
                        Q2_SNAP_STORE)
               .to(Q2_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q3SnapshotProcessor(Q3_SNAP_STORE,
                                                      Q3_P_LON, Q3_P_LAT, Q3_RADIUS_METRES, SNAPSHOT_TICK_MILLIS),
                        Q3_SNAP_STORE)
               .to(Q3_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.Integer()));

        tripsK0.process(() -> new Q4SnapshotProcessor(Q4_SNAP_WAS_INSIDE_STORE, Q4_SNAP_ENTRIES_STORE,
                                                       Q4_XMIN, Q4_YMIN, Q4_XMAX, Q4_YMAX, SNAPSHOT_TICK_MILLIS),
                        Q4_SNAP_WAS_INSIDE_STORE, Q4_SNAP_ENTRIES_STORE)
               .to(Q4_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q5SnapshotProcessor(Q5_SNAP_STORE,
                                                       Q5_P_LON, Q5_P_LAT, Q5_D_P_METRES, Q5_D_MEET_METRES,
                                                       SNAPSHOT_TICK_MILLIS),
                        Q5_SNAP_STORE)
               .to(Q5_SNAPSHOT_OUTPUT, Produced.with(Serdes.String(), Serdes.Double()));

        tripsK0.process(() -> new Q6SnapshotProcessor(Q6_SNAP_STORE, SNAPSHOT_TICK_MILLIS), Q6_SNAP_STORE)
               .to(Q6_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q7SnapshotProcessor(Q7_SNAP_STORE, Q7_POIS, SNAPSHOT_TICK_MILLIS), Q7_SNAP_STORE)
               .to(Q7_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.String()));

        tripsK0.process(() -> new Q8SnapshotProcessor(Q8_SNAP_STORE,
                                                       Q8_S1_LON, Q8_S1_LAT, Q8_S2_LON, Q8_S2_LAT,
                                                       Q8_RADIUS_METRES, SNAPSHOT_TICK_MILLIS),
                        Q8_SNAP_STORE)
               .to(Q8_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.Integer()));

        tripsK0.process(() -> new Q9SnapshotProcessor(Q9_SNAP_STORE,
                                                       Q9_X_VEHICLE_ID, Q9_Y_VEHICLE_ID, SNAPSHOT_TICK_MILLIS),
                        Q9_SNAP_STORE)
               .to(Q9_SNAPSHOT_OUTPUT, Produced.with(Serdes.Long(), Serdes.Double()));

        return builder.build();
    }

    private static <K, V> void addStore(StreamsBuilder builder, String name,
                                        org.apache.kafka.common.serialization.Serde<K> ks,
                                        org.apache.kafka.common.serialization.Serde<V> vs) {
        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<K, V>> sb =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(name), ks, vs);
        builder.addStateStore(sb);
    }
}
