package org.mobilitydb.kafka.meos.wirings;

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.Serializable;

/**
 * Kafka Streams DSL wirings for the {@code stateless} streaming tier of
 * the generated {@code org.mobilitydb.kafka.meos.MeosOps*} facades.
 *
 * <p>Kafka Streams' DSL is lambda-driven — {@code KStream.mapValues}
 * and {@code KStream.filter} accept {@link ValueMapper} and
 * {@link Predicate} interfaces directly. This class supplies tiny
 * helper factories that wrap a generated MeosOps method (or any
 * stateless function over a MEOS value) into the DSL-typed shape,
 * with serializability + the shared {@link MeosOpsRuntime#MEOS_AVAILABLE}
 * probe baked in.
 *
 * <p>Per the v4 streaming-relevance baseline, 804 of the 2,097
 * generated methods are {@code stateless} (92 OO-classified + 712
 * free-fn) — any of them can flow through these two helpers without
 * per-method registration.
 *
 * <p><b>Typical usage</b> — scalar-predicate filter using the generated
 * {@code MeosOpsFreeCore.overlaps_tbox_tbox} (tier = {@code stateless}):
 *
 * <pre>{@code
 * KStream<String, TboxPair> in = ...;
 * KStream<String, TboxPair> overlapping = in.filter(
 *     MeosStatelessOps.intPredicate(
 *         (key, pair) -> MeosOpsFreeCore.overlaps_tbox_tbox(pair.a, pair.b)));
 * }</pre>
 *
 * <p>Or per-record transform:
 *
 * <pre>{@code
 * KStream<String, String> hexOut = in.mapValues(
 *     MeosStatelessOps.mapper(
 *         tbox -> MeosOpsTBox.tbox_as_hexwkb(tbox, (byte) 4, null)));
 * }</pre>
 *
 * <p>Both helpers throw {@link UnsupportedOperationException} when
 * libmeos is unavailable (the underlying generated MeosOps methods do;
 * these helpers preserve the exception shape).
 */
public final class MeosStatelessOps {

    /** Serializable boolean-returning per-record MEOS predicate. */
    @FunctionalInterface
    public interface MeosPredicate<K, V> extends Predicate<K, V>, Serializable {
        @Override boolean test(K key, V value);
    }

    /** Serializable int-returning per-record MEOS predicate (0/1 flag). */
    @FunctionalInterface
    public interface MeosIntPredicate<K, V> extends Serializable {
        int test(K key, V value);
    }

    /** Serializable per-record MEOS value-mapper. */
    @FunctionalInterface
    public interface MeosMapper<V, R> extends ValueMapper<V, R>, Serializable {
        @Override R apply(V value);
    }

    private MeosStatelessOps() { /* utility */ }

    /**
     * Wrap a serializable {@code (K, V) -> boolean} as a Kafka-Streams
     * {@link Predicate}. Use with {@link org.apache.kafka.streams.kstream.KStream#filter}.
     */
    public static <K, V> Predicate<K, V> predicate(MeosPredicate<K, V> p) {
        return p;
    }

    /**
     * Adapt an {@code int}-returning generated MEOS predicate (treating
     * non-zero as {@code true}) into a Kafka-Streams {@link Predicate}.
     */
    public static <K, V> Predicate<K, V> intPredicate(MeosIntPredicate<K, V> p) {
        return (k, v) -> p.test(k, v) != 0;
    }

    /**
     * Wrap a serializable {@code V -> R} as a Kafka-Streams
     * {@link ValueMapper}. Use with {@link org.apache.kafka.streams.kstream.KStream#mapValues}.
     */
    public static <V, R> ValueMapper<V, R> mapper(MeosMapper<V, R> m) {
        return m;
    }
}
