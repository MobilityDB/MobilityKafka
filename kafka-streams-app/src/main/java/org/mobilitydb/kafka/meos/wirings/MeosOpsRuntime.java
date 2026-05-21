package org.mobilitydb.kafka.meos.wirings;

import org.mobilitydb.kafka.meos.MeosOpsTBox;

/**
 * Convenience re-export of the shared
 * {@code org.mobilitydb.kafka.meos.MeosOpsRuntime.MEOS_AVAILABLE} flag
 * for the wirings layer.
 *
 * <p>The codegen package already supplies the single
 * {@code MeosOpsRuntime} static initializer that probes libmeos exactly
 * once per JVM and exposes the result via every {@code MeosOps*.MEOS_AVAILABLE}
 * accessor. This class is just a wirings-package alias that lets wiring
 * code stay package-local without pulling in the full
 * {@code org.mobilitydb.kafka.meos.MeosOpsRuntime} type for what should
 * be a single boolean read.
 *
 * <p>The flag is checked inside every generated MeosOps method's body
 * (via a thrown {@link UnsupportedOperationException} when false), so
 * wiring code rarely needs to consult it explicitly — but when a
 * Kafka Streams topology wants to short-circuit before submitting
 * (e.g. a clean exit message at startup if libmeos isn't loadable),
 * {@code MeosOpsRuntime.MEOS_AVAILABLE} is the canonical place to read.
 */
public final class MeosOpsRuntime {

    /** Shared MEOS-available flag (set once per JVM by the codegen runtime). */
    public static final boolean MEOS_AVAILABLE = MeosOpsTBox.MEOS_AVAILABLE;

    private MeosOpsRuntime() { /* utility */ }
}
