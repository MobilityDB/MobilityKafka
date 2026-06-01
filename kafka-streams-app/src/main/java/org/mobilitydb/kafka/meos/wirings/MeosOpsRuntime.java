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
