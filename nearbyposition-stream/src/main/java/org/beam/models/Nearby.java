package org.beam.models;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.beam.dto.NearbyDTO;
import org.joda.time.Instant;

@DefaultCoder(AvroCoder.class)
public class Nearby {

    private int idU;
    private int idV;
    private long timestamp = Long.MAX_VALUE;

    public Nearby() {}

    public Nearby(int idU, int idV, long wTimestamp) {
        this.idU = Math.min(idU, idV);
        this.idV = Math.max(idU, idV);
        this.timestamp = wTimestamp;
    }

    public Nearby(long key) {
        this.idU = getIdUFromKey(key);
        this.idV = getIdVFromKey(key);
    }

    public NearbyDTO first(){
        return new NearbyDTO(idU, idV, timestamp, Instant.now().getMillis());
    }

    public NearbyDTO second(){
        return new NearbyDTO(idV, idU, timestamp, Instant.now().getMillis());
    }

    public long getKey(){
        return getKeyFromPair(idU, idV);
    }

    public static long getKeyFromPair(int idU, int idV) {
        long kU = Math.min(idU, idV);
        long kV = Math.max(idU, idV);
        return (kU << 32) + kV;
    }

    public static int getIdUFromKey(long key){
        return (int) (key >>> 32);
    }

    public static int getIdVFromKey(long key){
        return (int) ((key << 32) >>> 32);
    }

    public long getIdU() {
        return idU;
    }

    public long getIdV() {
        return idV;
    }

    public void setIdU(int idU) {
        this.idU = idU;
    }

    public void setIdV(int idV) {
        this.idV = idV;
    }

    @Override
    public String toString() {
        return idU + "-" + idV + " : ";
    }
}
