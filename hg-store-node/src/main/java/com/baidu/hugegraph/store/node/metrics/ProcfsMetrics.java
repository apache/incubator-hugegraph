package com.baidu.hugegraph.store.node.metrics;


import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * @author lynn.bond@hotmail.com on 2022/3/1
 * @version 0.1.0
 */
public class ProcfsMetrics {

    private static MeterRegistry registry;
    public final static String PREFIX = "process_memory";

    private final static ProcfsSmaps smaps = new ProcfsSmaps();;
    private ProcfsMetrics() {

    }

    public synchronized static void init(MeterRegistry meterRegistry) {
        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }
    }

    private static void registerMeters() {
        registerProcessGauge();
    }

    private static void registerProcessGauge() {
       Gauge.builder(PREFIX + ".rss.bytes",()-> smaps.get(ProcfsSmaps.KEY.RSS))
                .register(registry);

       Gauge.builder(PREFIX + ".pss.bytes",()-> smaps.get(ProcfsSmaps.KEY.PSS))
                .register(registry);

        Gauge.builder(PREFIX + ".vss.bytes",()-> smaps.get(ProcfsSmaps.KEY.VSS))
                .register(registry);

        Gauge.builder(PREFIX + ".swap.bytes",()-> smaps.get(ProcfsSmaps.KEY.SWAP))
                .register(registry);

        Gauge.builder(PREFIX + ".swappss.bytes",()-> smaps.get(ProcfsSmaps.KEY.SWAPPSS))
                .register(registry);
    }

}
