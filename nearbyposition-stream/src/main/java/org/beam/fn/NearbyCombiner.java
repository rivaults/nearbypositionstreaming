package org.beam.fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.beam.dto.NearbyDTO;
import org.beam.models.Nearby;

import java.util.Iterator;

public class NearbyCombiner extends DoFn<KV<Long, Iterable<Nearby>>, KV<Long, NearbyDTO>> {

    @ProcessElement
    public void processElement(@Element KV<Long, Iterable<Nearby>> group, OutputReceiver<KV<Long, NearbyDTO>> out) {
        assert group.getValue() != null;
        Iterator<Nearby> ite = group.getValue().iterator();
        if (ite.hasNext()) {
            Nearby res = ite.next();
            out.output(KV.of(res.getIdU(), res.first()));
            out.output(KV.of(res.getIdV(), res.second()));
        }
    }
}
