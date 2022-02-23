package com.baidu.hugegraph.metrics;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum MetricManager {

    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(MetricManager.class);

    private final MetricRegistry registry = new MetricRegistry();

    /**
     * Return the {@code MetricsRegistry}.
     *
     * @return the single {@code MetricRegistry} used for all of monitoring
     */
    public MetricRegistry getRegistry() {
        return registry;
    }
}
