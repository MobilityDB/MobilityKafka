package berlinmod;

/**
 * Distance from a (lon, lat) point to a (lon, lat) line segment, in metres,
 * via a local equirectangular projection centred on the segment midpoint.
 *
 * <p>Same shape as the MobilityFlink {@code SegmentDistance} utility so
 * BerlinMOD-Q8 produces byte-identical cross-platform output. TODO(meos):
 * replace with MEOS {@code distance(tgeompoint, geometry(LINESTRING))}
 * via the JMEOS bridge.
 */
public final class SegmentDistance {

    private static final double EARTH_RADIUS_METRES = 6_371_000.0;

    private SegmentDistance() {}

    public static double distanceMetres(
            double pLon, double pLat,
            double s1Lon, double s1Lat,
            double s2Lon, double s2Lat) {
        double midLat = (s1Lat + s2Lat) / 2.0;
        double mPerDegLat = Math.toRadians(1.0) * EARTH_RADIUS_METRES;
        double mPerDegLon = mPerDegLat * Math.cos(Math.toRadians(midLat));

        double px = pLon * mPerDegLon;
        double py = pLat * mPerDegLat;
        double s1x = s1Lon * mPerDegLon;
        double s1y = s1Lat * mPerDegLat;
        double s2x = s2Lon * mPerDegLon;
        double s2y = s2Lat * mPerDegLat;

        double dx = s2x - s1x;
        double dy = s2y - s1y;
        double lenSq = dx * dx + dy * dy;
        if (lenSq == 0.0) {
            return Math.hypot(px - s1x, py - s1y);
        }
        double t = ((px - s1x) * dx + (py - s1y) * dy) / lenSq;
        if (t < 0.0) t = 0.0;
        else if (t > 1.0) t = 1.0;
        double cx = s1x + t * dx;
        double cy = s1y + t * dy;
        return Math.hypot(px - cx, py - cy);
    }

    public static boolean withinMetres(
            double pLon, double pLat,
            double s1Lon, double s1Lat,
            double s2Lon, double s2Lat,
            double radiusMetres) {
        return distanceMetres(pLon, pLat, s1Lon, s1Lat, s2Lon, s2Lat) <= radiusMetres;
    }
}
