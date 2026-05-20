package Queries;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class AISData {
    private Long timestamp;
    private int mmsi;
    private double lon;
    private double lat;
    private double speed;
    private double course;

    // Getters and setters

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public int getMmsi() {
        return mmsi;
    }

    public void setMmsi(int mmsi) {
        this.mmsi = mmsi;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public double getCourse() {
        return course;
    }

    public void setCourse(double course) {
        this.course = course;
    }

    public static AISData fromCsv(String row) {
        String[] cols = row.split(",");
        AISData data = new AISData();
        data.setTimestamp(/* epoch millis from cols[0] */
                LocalDateTime.parse(cols[0].trim(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .toInstant(ZoneOffset.UTC).toEpochMilli());
        data.setMmsi(Integer.parseInt(cols[1].trim()));
        data.setLat(Double.parseDouble(cols[2].trim()));
        data.setLon(Double.parseDouble(cols[3].trim()));
        data.setSpeed(Double.parseDouble(cols[4].trim()));
        return data;
    }
}
