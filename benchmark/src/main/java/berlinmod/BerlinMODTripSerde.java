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
