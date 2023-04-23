package com.baidu.hugegraph.store.node.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author lynn.bond@hotmail.com on 2021/11/24
 */
@Configuration
public class MetricsConfig  {

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
        return (registry) -> registry.config().commonTags("hg", "store");
    }

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> registerMeters() {
        return (registry) -> {
            StoreMetrics.init(registry);
            RocksDBMetrics.init(registry);
            JRaftMetrics.init(registry);
            ProcfsMetrics.init(registry);
            GRpcExMetrics.init(registry);
        };
    }
    
}
