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

/**
 * BerlinMOD-Q2 — <b>continuous form</b>, Kafka-Streams Processor API.
 *
 * <p><i>"Where is vehicle X right now?"</i>
 *
 * <p>Pure stateless filter: forward records whose key matches the queried
 * {@code targetVehicleId}, drop the rest. Matches the MobilityFlink
 * {@code Q2ContinuousFunction} pattern.
 */
public class Q2ContinuousProcessor implements Processor<Integer, BerlinMODTrip, Integer, BerlinMODTrip> {

    private final int targetVehicleId;
    private ProcessorContext<Integer, BerlinMODTrip> ctx;

    public Q2ContinuousProcessor(int targetVehicleId) {
        this.targetVehicleId = targetVehicleId;
    }

    @Override
    public void init(ProcessorContext<Integer, BerlinMODTrip> context) {
        this.ctx = context;
    }

    @Override
    public void process(Record<Integer, BerlinMODTrip> record) {
        if (record.key() != null && record.key() == targetVehicleId) {
            ctx.forward(record);
        }
    }
}
