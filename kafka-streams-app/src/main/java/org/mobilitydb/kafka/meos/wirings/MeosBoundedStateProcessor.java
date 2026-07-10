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

import jnr.ffi.Pointer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.Serializable;

/**
 * Kafka Streams wiring for the {@code bounded-state} streaming tier of
 * the generated {@code org.mobilitydb.meos.MeosOps*} facades.
 *
 * <p>Wraps any {@code bounded-state} MeosOps method (per the v4 baseline:
 * 797 of 2,097 emitted methods — 513 OO-classified + 284 free-fn) as a
 * Kafka Streams {@link Processor} that holds per-key MEOS-handle state
 * across records via a {@link KeyValueStore}.
 *
 * <p><b>Why state lives as bytes, not as a {@code Pointer}.</b> Mirrors
 * the same discipline as the Flink wirings: a {@code jnr.ffi.Pointer}
 * is a raw native-memory address. It does not survive across Kafka
 * Streams' state-store fault-tolerance / changelog-replay /
 * task-rebalance paths (the state-store changelog is a Kafka topic;
 * state must be byte-serializable to be replayable). The wiring stores
 * state as {@code byte[]} (typically MEOS-WKB or MEOS-WKT — adopter's
 * choice) with three adopter-supplied lambdas mediating the round-trip:
 *
 * <pre>{@code
 *  byte[] state                  -- per-key serialized MEOS value (in state store)
 *      ↓ deserialize (bytes → Pointer)
 *  Pointer prev                  -- in-flight MEOS handle
 *      ↓ step(prev, record) → (newPointer, output)
 *  Pointer next, OUT out         -- new in-flight handle + per-record output
 *      ↓ serialize (Pointer → bytes)
 *  byte[] newState               -- new per-key serialized MEOS value (back to store)
 * }</pre>
 *
 * <p>First record for a key sees {@code prior == null} — the wiring
 * skips deserialize and lets the step seed state.
 *
 * <p><b>Typical usage</b> — per-vehicle running tbox union via
 * {@code MeosOpsFreeCore.union_tbox_tbox}:
 *
 * <pre>{@code
 * Topology topology = new Topology();
 * topology.addSource("src", ...);
 * topology.addProcessor(
 *     "running-union",
 *     () -> new MeosBoundedStateProcessor<String, VehicleEvent, String, RunningTbox>(
 *         "running-union-state",
 *         ptr -> MeosOpsTBox.tbox_out(ptr, 6).getBytes(StandardCharsets.UTF_8),
 *         bytes -> MeosOpsTBox.tbox_in(new String(bytes, StandardCharsets.UTF_8)),
 *         (prior, record) -> { ... return new MeosStep<>(newState, record.withValue(...)); }),
 *     "src");
 * topology.addStateStore(
 *     Stores.keyValueStoreBuilder(
 *         Stores.persistentKeyValueStore("running-union-state"),
 *         Serdes.String(), Serdes.ByteArray()),
 *     "running-union");
 * }</pre>
 *
 * @param <KIn>  key type
 * @param <VIn>  input record value type
 * @param <KOut> output key type (typically same as KIn)
 * @param <VOut> output value type
 */
public final class MeosBoundedStateProcessor<KIn, VIn, KOut, VOut>
        implements Processor<KIn, VIn, KOut, VOut> {

    /** Serializable Pointer → bytes serializer (typically MEOS-WKB or MEOS-WKT). */
    @FunctionalInterface
    public interface PointerSerialize extends Serializable {
        byte[] toBytes(Pointer pointer);
    }

    /** Serializable bytes → Pointer deserializer (typically MEOS-WKB or MEOS-WKT). */
    @FunctionalInterface
    public interface PointerDeserialize extends Serializable {
        Pointer fromBytes(byte[] bytes);
    }

    /** Per-record step: (prior MEOS handle, input record) → (new handle, optional output record). */
    @FunctionalInterface
    public interface MeosStepFn<KIn, VIn, KOut, VOut> extends Serializable {
        MeosStep<KOut, VOut> apply(Pointer prior, Record<KIn, VIn> record);
    }

    /** Tuple returned by the step lambda. */
    public static final class MeosStep<KOut, VOut> {
        public final Pointer newState;
        public final Record<KOut, VOut> output;   // null = no forward
        public MeosStep(Pointer newState, Record<KOut, VOut> output) {
            this.newState = newState;
            this.output = output;
        }
    }

    private final String stateStoreName;
    private final PointerSerialize serialize;
    private final PointerDeserialize deserialize;
    private final MeosStepFn<KIn, VIn, KOut, VOut> step;

    private KeyValueStore<KIn, byte[]> store;
    private ProcessorContext<KOut, VOut> context;

    public MeosBoundedStateProcessor(String stateStoreName,
                                     PointerSerialize serialize,
                                     PointerDeserialize deserialize,
                                     MeosStepFn<KIn, VIn, KOut, VOut> step) {
        this.stateStoreName = stateStoreName;
        this.serialize = serialize;
        this.deserialize = deserialize;
        this.step = step;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.store = context.getStateStore(stateStoreName);
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        byte[] priorBytes = store.get(record.key());
        Pointer prior = (priorBytes == null) ? null : deserialize.fromBytes(priorBytes);

        MeosStep<KOut, VOut> stepResult = step.apply(prior, record);

        store.put(record.key(), serialize.toBytes(stepResult.newState));

        if (stepResult.output != null) {
            context.forward(stepResult.output);
        }
    }

    @Override
    public void close() { /* nothing to release; MEOS handles are short-lived per record */ }
}
