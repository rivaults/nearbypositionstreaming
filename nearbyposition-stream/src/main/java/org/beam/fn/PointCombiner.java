package org.beam.fn;

import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.beam.models.Nearby;
import org.beam.models.Location;

import java.util.*;

import static org.beam.models.Nearby.getKeyFromPair;

public class PointCombiner extends DoFn<KV<Long, Iterable<Location>>, KV<Long, Nearby>> {

    @StateId("in")
    private final StateSpec<SetState<Location>> stateIn = StateSpecs.set();

    @StateId("results")
    private final StateSpec<SetState<Long>> stateResults = StateSpecs.set();


    @ProcessElement
    public void processElement(@Element KV<Long, Iterable<Location>> group,
                               OutputReceiver<KV<Long, Nearby>> out,
                               BoundedWindow window,
                               @AlwaysFetched @StateId("in") SetState<Location> stateIn,
                               @AlwaysFetched @StateId("results") SetState<Long> stateResults,
                               ProcessContext context) {
        assert group.getKey() != null;
        assert group.getValue() != null;
        if (context.pane().isFirst()) {
            stateResults.clear();
            stateIn.clear();
        }
        Iterable<Location> in = Objects.requireNonNull(stateIn.read());
        for(Location u : group.getValue()){
            for (Location v : in) {
                if (u.getId() != v.getId() && u.shouldCollide(v, group.getKey())) {
                    long key = getKeyFromPair(u.getId(), v.getId());
                    out.output(KV.of(key, new Nearby(u.getId(), v.getId(), window.maxTimestamp().getMillis())));
                    stateResults.addIfAbsent(key);
                }
            }
            stateIn.addIfAbsent(u);
        }
    }
}