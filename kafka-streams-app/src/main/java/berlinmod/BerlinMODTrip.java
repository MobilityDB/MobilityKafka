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
    private String h3Cell; // precomputed H3 cell of the event point (from the dataset)

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
    public String getH3Cell() { return h3Cell; }
    public void setH3Cell(String h3Cell) { this.h3Cell = h3Cell; }
}
