package org.beam.fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log<T> extends DoFn<T, T> {

    private final String prefix;

    public Log(String p) {
        prefix = p;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Log.class);

    @ProcessElement
    public void processElement(@Element T kv, OutputReceiver<T> out, BoundedWindow window){
        LOG.info("{} {} : {}", prefix, window.hashCode(), kv);
        out.output(kv);
    }
}
