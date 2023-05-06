package org.apache.hugegraph.pd.metrics;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lynn.bond@hotmail.com on 2022/1/5
 */
@Component
@Slf4j
public final class PDMetrics {
    public final static String PREFIX = "hg";
    private final static AtomicLong graphs = new AtomicLong(0);
    private MeterRegistry registry;

    @Autowired
    private PDService pdService;

    public synchronized void init(MeterRegistry meterRegistry) {

        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }

    }

    private void registerMeters() {
        Gauge.builder(PREFIX + ".up", () -> 1).register(registry);

        Gauge.builder(PREFIX + ".graphs", () -> updateGraphs())
                .description("Number of graphs registered in PD")
                .register(registry);

        Gauge.builder(PREFIX + ".stores", () -> updateStores())
                .description("Number of stores registered in PD")
                .register(registry);

    }

    private long  updateGraphs() {
        long buf = getGraphs();

        if (buf != graphs.get()) {
            graphs.set(buf);
            registerGraphMetrics();
        }
        return buf;
    }

    private long  updateStores() {
      return getStores();
    }

    private long getGraphs() {
        return getGraphMetas().size();
    }

    private long getStores(){
        try {
            return this.pdService.getStoreNodeService().getStores(null).size();
        } catch (PDException e) {
            log.error(e.getMessage(),e);
            e.printStackTrace();
        }
        return 0;
    }

    private List<Metapb.Graph> getGraphMetas(){
        try {
            return this.pdService.getPartitionService().getGraphs();
        } catch (PDException e) {
            log.error(e.getMessage(),e);
        }
        return Collections.EMPTY_LIST;
    }

    private void registerGraphMetrics(){
        this.getGraphMetas().forEach(meta->{
            Gauge.builder(PREFIX + ".partitions",this.pdService.getPartitionService()
                    ,e-> e.getPartitions(meta.getGraphName()).size())
                    .description("Number of partitions assigned to a graph")
                    .tag("graph",meta.getGraphName())
                    .register(this.registry);

        });
    }

}
