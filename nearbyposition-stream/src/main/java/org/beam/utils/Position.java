package org.beam.utils;

import org.beam.models.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Position {

    private final static Random random = new Random(42);
    private final static double grid_w =  2*0.0001d;
    private final static double origin_x =  random.nextDouble()*grid_w;
    private final static double origin_y =  random.nextDouble()*grid_w;
    private final static long rndA =  random.nextLong();
    private final static long rndB =  random.nextLong();
    private final static long rndC =  random.nextInt();

    private static final Logger LOG = LoggerFactory.getLogger(Position.class);

    private static long hash(long x, long y) {
        return (x*Position.rndA >>> 32) + (y*Position.rndB >>> 32) + Position.rndC;
    }

    public static HashSet<Long> getPositions(Location location) {
        double g_x = (location.getPoint().getLatitude() - origin_x) / grid_w;
        double g_y = (location.getPoint().getLongitude() - origin_y) / grid_w;
        HashSet<Long> results = new HashSet<>();
        results.add(hash((long) Math.floor(g_x), (long) Math.floor(g_y)));
        results.add(hash((long) Math.floor(g_x), (long) Math.ceil(g_y)));
        results.add(hash((long) Math.ceil(g_x), (long) Math.floor(g_y)));
        results.add(hash((long) Math.ceil(g_x), (long) Math.ceil(g_y)));
        return results;
    }
}
