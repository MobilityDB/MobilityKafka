package berlinmod;

import java.io.Serializable;

/**
 * Plain data class for a single GPS event from a BerlinMOD trip.
 *
 * <p>Same shape as the MobilityFlink {@code BerlinMODTrip} so a CSV produced
 * by the BerlinMOD generator at any SF feeds both pipelines unchanged.
 */
public class BerlinMODTrip implements Serializable {

    private static final long serialVersionUID = 1L;

    private long timestamp; // epoch milliseconds (event time)
    private int vehicleId;
    private double lon;
    private double lat;

    public BerlinMODTrip() {}

    public BerlinMODTrip(int vehicleId, long timestamp, double lon, double lat) {
        this.vehicleId = vehicleId;
        this.timestamp = timestamp;
        this.lon = lon;
        this.lat = lat;
    }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public int getVehicleId() { return vehicleId; }
    public void setVehicleId(int vehicleId) { this.vehicleId = vehicleId; }
    public double getLon() { return lon; }
    public void setLon(double lon) { this.lon = lon; }
    public double getLat() { return lat; }
    public void setLat(double lat) { this.lat = lat; }
}
