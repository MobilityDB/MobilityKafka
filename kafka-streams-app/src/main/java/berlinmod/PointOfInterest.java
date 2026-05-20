package berlinmod;

import java.io.Serializable;

/**
 * Point-of-interest record used by BerlinMOD-Q7: an integer id, a (lon, lat)
 * location, and a proximity radius in metres.
 */
public final class PointOfInterest implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int id;
    public final double lon;
    public final double lat;
    public final double radiusMetres;

    public PointOfInterest(int id, double lon, double lat, double radiusMetres) {
        this.id = id;
        this.lon = lon;
        this.lat = lat;
        this.radiusMetres = radiusMetres;
    }
}
