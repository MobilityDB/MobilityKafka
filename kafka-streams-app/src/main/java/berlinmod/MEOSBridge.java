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

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;

/**
 * Thin wiring from the BerlinMOD streaming-form predicates to MEOS via JMEOS.
 *
 * <p>Every spatial predicate evaluates through MEOS: the within-distance
 * predicate is the canonical temporal operator {@code edwithin_tgeo_geo} —
 * ever-within between the vehicle's {@code tgeogpoint} instant and the query
 * geography, in metres on the WGS84 spheroid; region containment is
 * {@code eintersects_tgeo_geo} between the point's {@code tgeompoint} instant
 * and the region polygon; distances are {@code geog_distance}. This class holds
 * no spatial mathematics of its own: it constructs the MEOS inputs and delegates
 * the computation to libmeos, initialising MEOS once per stream thread.
 */
public final class MEOSBridge {

    private static final ThreadLocal<Boolean> INITIALIZED =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    private MEOSBridge() {
        // utility
    }

    private static void ensureInitializedOnThread() {
        if (!INITIALIZED.get()) {
            GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
            GeneratedFunctions.meos_initialize();
            INITIALIZED.set(Boolean.TRUE);
        }
    }

    /** @return {@code true} iff {@code (lon1, lat1)} is within {@code radiusMetres}
     *  of {@code (lon2, lat2)} on the WGS84 spheroid, via MEOS {@code edwithin_tgeo_geo}. */
    public static boolean dwithinMetres(double lon1, double lat1,
                                        double lon2, double lat2, double radiusMetres) {
        ensureInitializedOnThread();
        return GeneratedFunctions.edwithin_tgeo_geo(
                tgeogInst(lon1, lat1), pointGeog(lon2, lat2), radiusMetres) == 1;
    }

    /** @return {@code true} iff {@code (pLon, pLat)} is within {@code radiusMetres}
     *  of the LineString {@code (s1, s2)}, via MEOS {@code edwithin_tgeo_geo}. */
    public static boolean dwithinSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat,
                                               double s2Lon, double s2Lat, double radiusMetres) {
        ensureInitializedOnThread();
        return GeneratedFunctions.edwithin_tgeo_geo(
                tgeogInst(pLon, pLat), lineGeog(s1Lon, s1Lat, s2Lon, s2Lat), radiusMetres) == 1;
    }

    /** @return {@code true} iff {@code (lon, lat)} lies in the axis-aligned box, via
     *  MEOS {@code eintersects_tgeo_geo} against the box polygon (planar, SRID 4326). */
    public static boolean intersectsBox(double lon, double lat,
                                        double xmin, double ymin, double xmax, double ymax) {
        ensureInitializedOnThread();
        return GeneratedFunctions.eintersects_tgeo_geo(
                tgeomInst(lon, lat), boxPolygon(xmin, ymin, xmax, ymax)) == 1;
    }

    /** @return the WGS84 spheroidal distance in metres between two points, via MEOS {@code geog_distance}. */
    public static double distanceMetres(double lon1, double lat1, double lon2, double lat2) {
        ensureInitializedOnThread();
        return GeneratedFunctions.geog_distance(pointGeog(lon1, lat1), pointGeog(lon2, lat2));
    }

    /** @return the WGS84 spheroidal distance in metres from a point to the LineString,
     *  via MEOS {@code geog_distance}. */
    public static double distanceSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat, double s2Lon, double s2Lat) {
        ensureInitializedOnThread();
        return GeneratedFunctions.geog_distance(
                pointGeog(pLon, pLat), lineGeog(s1Lon, s1Lat, s2Lon, s2Lat));
    }

    private static Pointer tgeogInst(double lon, double lat) {
        return GeneratedFunctions.tgeogpoint_in(
                String.format("SRID=4326;Point(%.7f %.7f)@2000-01-01", lon, lat));
    }

    private static Pointer tgeomInst(double lon, double lat) {
        return GeneratedFunctions.tgeompoint_in(
                String.format("SRID=4326;Point(%.7f %.7f)@2000-01-01", lon, lat));
    }

    private static Pointer pointGeog(double lon, double lat) {
        return GeneratedFunctions.geom_to_geog(
                GeneratedFunctions.geom_in(String.format("SRID=4326;Point(%.7f %.7f)", lon, lat), -1));
    }

    private static Pointer lineGeog(double s1Lon, double s1Lat, double s2Lon, double s2Lat) {
        return GeneratedFunctions.geom_to_geog(GeneratedFunctions.geom_in(String.format(
                "SRID=4326;LineString(%.7f %.7f, %.7f %.7f)", s1Lon, s1Lat, s2Lon, s2Lat), -1));
    }

    private static Pointer boxPolygon(double xmin, double ymin, double xmax, double ymax) {
        return GeneratedFunctions.geom_in(String.format(
                "SRID=4326;Polygon((%.7f %.7f, %.7f %.7f, %.7f %.7f, %.7f %.7f, %.7f %.7f))",
                xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin), -1);
    }
}
