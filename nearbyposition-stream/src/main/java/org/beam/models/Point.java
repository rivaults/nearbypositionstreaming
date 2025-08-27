package org.beam.models;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class Point implements Comparable<Point> {

    private long id;
    private double latitude;
    private double longitude;
    private int altitude;
    private long event;

    public Point() {
    }

    public Point(long id, double latitude, double longitude, int altitude, long event) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.event = event;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public int getAltitude() {
        return altitude;
    }

    public void setAltitude(int altitude) {
        this.altitude = altitude;
    }

    public long getEvent() {
        return event;
    }

    public void setEvent(long event) {
        this.event = event;
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return id == point.id && Double.compare(latitude, point.latitude) == 0 && Double.compare(longitude, point.longitude) == 0 && altitude == point.altitude && event == point.event;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, latitude, longitude, altitude, event);
    }

    @Override
    public int compareTo(@NotNull Point o) {
        return Long.compareUnsigned(this.getId(), o.getId());
    }
}
