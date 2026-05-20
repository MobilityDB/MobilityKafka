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
