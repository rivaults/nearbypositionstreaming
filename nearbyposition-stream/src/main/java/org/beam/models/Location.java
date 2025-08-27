package org.beam.models;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class Location {

    private int id;
    private Point point;
    private HashSet<Long> cells;

    public Location() {}

    public int getId() {
        return id;
    }

    public Location(int id, Point location) {
        this.id = id;
        this.point = location;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Point getPoint() {
        return point;
    }

    public void setCells(HashSet<Long> cells) {
        this.cells = cells;
    }

    public HashSet<Long> getCells() {
        return cells;
    }

    public boolean shouldCollide(Location location, long currentKey) {
        HashSet<Long> res = new HashSet<>(cells);
        res.retainAll(location.cells);
        return Collections.min(res) == currentKey;
    }

    public static Location getLocation(GenericRecord record){
        int tId = (int) record.get("id");
        long pId = (long) record.get("timestamp");
        double latitude = (double) record.get("latitude");
        double longitude = (double) record.get("longitude");
        int altitude = (int) record.get("altitude");
        long event = (long) record.get("event");
        Point point = new Point(pId, latitude, longitude, altitude, event);
        return new Location(tId, point);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Location location1 = (Location) o;
        return id == location1.id && Objects.equals(point, location1.point);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, point.getId());
    }

    @Override
    public String toString() {
        return id + "_" + point.getId();
    }
}
