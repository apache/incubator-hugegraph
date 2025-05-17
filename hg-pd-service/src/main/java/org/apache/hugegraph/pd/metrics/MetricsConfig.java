package org.apache.hugegraph.pd.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author lynn.bond@hotmail.com on 2022/01/05
 */
@Configuration
public class MetricsConfig {
    @Autowired
    private PDMetrics metrics;

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
        return (registry) -> registry.config().commonTags("hg", "pd");
    }

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> registerMeters() {
        return (registry) -> {
            metrics.init(registry);
        };
    }

}
