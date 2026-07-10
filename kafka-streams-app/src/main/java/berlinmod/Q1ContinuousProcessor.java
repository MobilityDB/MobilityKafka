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

package berlinmod;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q1 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Which vehicles have appeared in the stream?"</i>
 *
 * <p>Uses a {@link KeyValueStore} keyed by {@code vehicleId} (named in the
 * caller) to dedupe — emits {@code (vehicleId, firstSeenTimestamp)} the
 * first time each vehicle is seen and ignores subsequent events.
 *
 * <p>Same semantic as the MobilityFlink {@code Q1ContinuousFunction}; the
 * differences are purely in the runtime API (Kafka Streams Processor vs
 * Flink {@code KeyedProcessFunction}).
 */
public class Q1ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, Long> {

    private static final Logger LOG = LoggerFactory.getLogger(Q1ContinuousProcessor.class);

    private final String storeName;
    private KeyValueStore<Integer, Boolean> seen;
    private ProcessorContext<Integer, Long> ctx;

    public Q1ContinuousProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Integer, Long> context) {
        this.ctx = context;
        this.seen = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        Integer vehicleId = record.key();
        BerlinMODTrip trip = record.value();
        if (vehicleId == null || trip == null || vehicleId == -1) return;

        Boolean alreadySeen = seen.get(vehicleId);
        if (alreadySeen == null || !alreadySeen) {
            seen.put(vehicleId, true);
            ctx.forward(new Record<>(vehicleId, trip.getTimestamp(), trip.getTimestamp()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Q1-continuous first-sighting: vehicle={} t={}", vehicleId, trip.getTimestamp());
            }
        }
    }
}
