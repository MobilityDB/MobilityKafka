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

package org.mobilitydb.kafka.meos.wirings;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

import java.io.Serializable;

/**
 * Kafka Streams wiring for the {@code windowed} streaming tier of the
 * generated {@code org.mobilitydb.kafka.meos.MeosOps*} facades.
 *
 * <p>The {@code windowed} tier (per the v4 baseline: 161 of 2,097
 * emitted methods) emits one MEOS-derived value per window. Canonical
 * examples are {@code temporal_length(tgeo)} (one length per
 * trajectory window) and {@code temporal_twavg(tnumber)} (one
 * time-weighted average per window).
 *
 * <p>Kafka Streams' {@code KStream.groupByKey().windowedBy(...).aggregate(
 *     Initializer<A>, Aggregator<K, V, A>, Materialized<...>)}
 * builder is lambda-shaped; this helper supplies serializable
 * {@link Initializer} and {@link Aggregator} factories that:
 *
 * <ul>
 *   <li>Initialize with a serializable seed value (typically empty
 *       state — an empty MEOS-WKB byte array or a sentinel).</li>
 *   <li>Aggregate per-event into a running accumulator value that
 *       Kafka Streams stores in the windowed state store as
 *       byte-serializable bytes.</li>
 *   <li>(Optional final-projection — Kafka Streams emits the running
 *       accumulator as the window output; if a different per-window
 *       projection is needed, apply it via {@code KStream.mapValues}
 *       on the windowed stream.)</li>
 * </ul>
 *
 * <p><b>Window-close vs running-emit.</b> Unlike Flink's
 * {@code ProcessWindowFunction} which fires once at window close,
 * Kafka Streams' suppress-less default emits a record per update.
 * For window-close-only semantics, chain {@code .suppress(Suppressed.untilWindowCloses(...))}
 * downstream — out of scope for this helper, but the standard recipe.
 *
 * <p>State-store discipline: same as {@link MeosBoundedStateProcessor} —
 * raw {@code Pointer} doesn't survive changelog replay; the aggregator
 * value type {@code A} should be byte-serializable
 * (typically {@code byte[]} encoded MEOS-WKB / MEOS-WKT). Adopters
 * configure the corresponding {@code Serde<A>} via
 * {@code Materialized.with(keySerde, valueSerde)}.
 *
 * @param <K> key type
 * @param <V> input record value type
 * @param <A> aggregator accumulator type (byte-serializable; typically byte[])
 */
public final class MeosWindowedAggregator {

    private MeosWindowedAggregator() { /* utility */ }

    /** Serializable per-window initial-value supplier. */
    @FunctionalInterface
    public interface MeosInitializer<A> extends Initializer<A>, Serializable {
        @Override A apply();
    }

    /** Serializable per-event accumulator step. */
    @FunctionalInterface
    public interface MeosAggregator<K, V, A> extends Aggregator<K, V, A>, Serializable {
        @Override A apply(K key, V value, A aggregate);
    }

    /**
     * Wrap a serializable {@code () -> A} as a Kafka-Streams
     * {@link Initializer}.
     */
    public static <A> Initializer<A> initializer(MeosInitializer<A> init) {
        return init;
    }

    /**
     * Wrap a serializable {@code (K, V, A) -> A} as a Kafka-Streams
     * {@link Aggregator}.
     *
     * <p>The lambda receives the current key, the per-event value, and
     * the running aggregator state; returns the new aggregator state.
     * Per the wirings discipline, the {@code A} type should be
     * byte-serializable so changelog-replay / state-rebalance work
     * across Kafka Streams' fault-tolerance paths.
     */
    public static <K, V, A> Aggregator<K, V, A> aggregator(MeosAggregator<K, V, A> agg) {
        return agg;
    }
}
