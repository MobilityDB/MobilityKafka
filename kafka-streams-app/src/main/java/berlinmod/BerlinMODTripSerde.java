package berlinmod;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON-based Kafka Streams {@link Serde} for {@link BerlinMODTrip}.
 *
 * <p>Records on the {@code berlinmod} topic are JSON objects of the form
 * {@code {"t": "yyyy-MM-dd HH:mm:ss", "vehicle_id": 42, "lon": 4.36, "lat":
 * 50.84}}. The serializer writes that exact shape; the deserializer parses
 * the same shape (with a tolerant timestamp format matching the producer).
 */
public final class BerlinMODTripSerde implements Serde<BerlinMODTrip> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Serializer<BerlinMODTrip> serializer() {
        return (topic, trip) -> {
            if (trip == null) return null;
            try {
                return MAPPER.writeValueAsBytes(new SerForm(trip));
            } catch (Exception e) {
                throw new RuntimeException("BerlinMODTrip serialize failed", e);
            }
        };
    }

    @Override
    public Deserializer<BerlinMODTrip> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                SerForm sf = MAPPER.readValue(bytes, SerForm.class);
                return sf.toTrip();
            } catch (Exception e) {
                throw new RuntimeException("BerlinMODTrip deserialize failed", e);
            }
        };
    }

    /** Wire form used for JSON serialisation — keeps Jackson happy without per-field annotations. */
    private static final class SerForm {
        public Long t;
        public int vehicle_id;
        public double lon;
        public double lat;

        public SerForm() {}

        SerForm(BerlinMODTrip trip) {
            this.t = trip.getTimestamp();
            this.vehicle_id = trip.getVehicleId();
            this.lon = trip.getLon();
            this.lat = trip.getLat();
        }

        BerlinMODTrip toTrip() {
            return new BerlinMODTrip(vehicle_id, t == null ? 0L : t, lon, lat);
        }
    }
}
