package org.mobilitydb.kafka.meos.wirings;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.io.Serializable;

/**
 * Kafka Streams wiring for the {@code cross-stream} streaming tier of
 * the generated {@code org.mobilitydb.kafka.meos.MeosOps*} facades.
 *
 * <p>The {@code cross-stream} tier (140 of 2,097 emitted methods per
 * v4 baseline) is pairwise across two pre-keyed streams within a
 * time-bounded match window. Canonical examples are spatial
 * relations between two trajectories
 * ({@code edwithin_tgeo_tgeo}, {@code eintersects_tgeo_tgeo}) and
 * distance functions on two temporals
 * ({@code nad_tgeo_tgeo}, {@code mindistance_tgeo_tgeo}).
 *
 * <p>Kafka Streams' {@code KStream.join(otherStream, joiner,
 *     JoinWindows.of(...))} builder calls a {@link ValueJoiner} per
 * matched pair; this helper supplies a serializable factory wrapping
 * an adopter lambda that calls the MEOS cross-stream method on the
 * pair.
 *
 * <p><b>Typical usage</b> — per-vehicle-pair "did they come within
 * 100m of each other in the last 5 minutes?" via
 * {@code MeosOpsTGeo.edwithin_tgeo_tgeo}:
 *
 * <pre>{@code
 * KStream<Integer, VehiclePosition> a = ...;   // keyed by regionId
 * KStream<Integer, VehiclePosition> b = ...;   // same key space
 *
 * KStream<Integer, MeetingEvent> meetings = a.join(
 *     b,
 *     MeosCrossStreamJoiner.joiner((left, right) -> {
 *         Pointer leftT  = left.toTGeoPointer();
 *         Pointer rightT = right.toTGeoPointer();
 *         if (MeosOpsTGeo.edwithin_tgeo_tgeo(leftT, rightT, 100.0) != 0) {
 *             return new MeetingEvent(left.id(), right.id(), System.currentTimeMillis());
 *         }
 *         return null;  // joined out
 *     }),
 *     JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)));
 * }</pre>
 *
 * <p>The join is keyed (both streams must share key space, and only
 * records sharing a key are considered for pairing). The match window
 * is time-bounded via {@code JoinWindows.ofTimeDifferenceWithNoGrace(...)}
 * (or the grace-period variant), event-time-aware.
 *
 * <p>For non-matches, the lambda can return {@code null} and chain
 * a downstream {@code .filter((k, v) -> v != null)} — Kafka Streams
 * forwards null values through the joiner; the filter prunes them.
 */
public final class MeosCrossStreamJoiner {

    /** Serializable per-match MEOS pairwise call. */
    @FunctionalInterface
    public interface MeosJoinFn<L, R, OUT> extends ValueJoiner<L, R, OUT>, Serializable {
        @Override OUT apply(L left, R right);
    }

    private MeosCrossStreamJoiner() { /* utility */ }

    /**
     * Wrap a serializable {@code (L, R) -> OUT} as a Kafka-Streams
     * {@link ValueJoiner}. Use with {@link org.apache.kafka.streams.kstream.KStream#join(
     *     org.apache.kafka.streams.kstream.KStream,
     *     ValueJoiner, org.apache.kafka.streams.kstream.JoinWindows)}.
     */
    public static <L, R, OUT> ValueJoiner<L, R, OUT> joiner(MeosJoinFn<L, R, OUT> fn) {
        return fn;
    }
}
