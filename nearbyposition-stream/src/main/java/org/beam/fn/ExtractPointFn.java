package org.beam.fn;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.beam.models.Location;
import org.beam.utils.Position;

public class ExtractPointFn extends DoFn<KV<String, GenericRecord>, KV<Long, Location>> {

    @ProcessElement
    public void processElement(@Element KV<String, GenericRecord> record, OutputReceiver<KV<Long, Location>> out) {
        assert record.getValue() != null;
        Location location = Location.getLocation(record.getValue());
        location.setCells(Position.getPositions(location));
        for(Long pos : Position.getPositions(location))
            out.output(KV.of(pos, location));
    }
}
