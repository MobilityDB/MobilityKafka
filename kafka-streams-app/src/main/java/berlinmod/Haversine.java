package berlinmod;

/**
 * Great-circle distance in metres between two WGS84 (lon, lat) points.
 *
 * <p>Matches the MobilityFlink {@code Haversine} utility verbatim so the
 * spatial-predicate cells (Q3 in this PR; Q4–Q9 in follow-ups) produce
 * cross-platform byte-identical output against the same synthetic corpus.
 * TODO(meos): cells using this utility migrate to the MEOS equivalent
 * (edwithin_tgeo_geo / NAD / etc.) via JMEOS once the in-flight 1.4 bump
 * signals settled.
 */
public final class Haversine {

    private static final double EARTH_RADIUS_METRES = 6_371_000.0;

    private Haversine() {}

    public static double distanceMetres(double lon1, double lat1, double lon2, double lat2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dPhi = Math.toRadians(lat2 - lat1);
        double dLambda = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                 + Math.cos(phi1) * Math.cos(phi2)
                 * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_METRES * c;
    }

    public static boolean withinMetres(double lon, double lat, double pLon, double pLat, double radiusMetres) {
        return distanceMetres(lon, lat, pLon, pLat) <= radiusMetres;
    }
}
