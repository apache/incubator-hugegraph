package org.apache.hugegraph.pd.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.model.GraphStatistics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2022/1/5
 */
@Component
@Slf4j
public final class PDMetrics {

    public final static String PREFIX = "hg";
    private static AtomicLong graphs = new AtomicLong(0);
    private static Map<String, Counter> lastTerms = new ConcurrentHashMap();
    private static Map<String, Gauge> leaderCountGauges = new ConcurrentHashMap();
    private static Map<String, Integer> leaderCounts = new ConcurrentHashMap();
    @Autowired
    PDRestService pdRestService;
    @Autowired
    private PDService pdService;
    private MeterRegistry registry;
    private Map<String, Pair<Long, Long>> lasts = new ConcurrentHashMap();
    private int interval = 120 * 1000;
    private volatile int avgLeaderCount = 0;
    private Function<String, Gauge> gaugeFunction = k -> Gauge.builder(PREFIX + ".store.leader.count",
                                                                       () -> leaderCounts.getOrDefault(k, 0))
                                                              .description("leader count of node")
                                                              .tag("address", k)
                                                              .register(this.registry);

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
        Gauge.builder(PREFIX + ".terms", () -> setTerms())
             .description("term of partitions in PD")
             .register(registry);
        Gauge.builder(PREFIX + ".store.leader.averageCount", () -> avgLeaderCount)
             .description("term of partitions in PD")
             .register(registry);
    }

    private long updateGraphs() {
        long buf = getGraphs();
        if (buf != graphs.get()) {
            graphs.set(buf);
            registerGraphMetrics();
        }
        return buf;
    }

    private long updateStores() {
        return getStores();
    }

    private long getGraphs() {
        return getGraphMetas().size();
    }

    private long getStores() {
        try {
            return this.pdService.getStoreNodeService().getStores(null).size();
        } catch (PDException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return 0;
    }

    private long setTerms() {
        List<ShardGroup> groups = null;
        try {
            groups = pdRestService.getShardGroups();
            StoreNodeService nodeService = pdService.getStoreNodeService();
            List<Metapb.Store> activeStores = nodeService.getActiveStores();
            HashMap<Long, Metapb.Store> stores = new HashMap<>(activeStores.size());
            for (Metapb.Store s : activeStores) {
                stores.put(s.getId(), s);
                leaderCountGauges.computeIfAbsent(s.getAddress(), gaugeFunction);
            }
            HashMap<Long, Integer> leaders = new HashMap<>();
            if (!MapUtils.isEmpty(stores)) {
                avgLeaderCount = (int) Math.ceil((double) groups.size() / (double) stores.size());
            }
            for (ShardGroup g : groups) {
                String id = String.valueOf(g.getId());
                ShardGroup group = nodeService.getShardGroup(g.getId());
                long version = group.getVersion();
                Counter lastTerm = lastTerms.get(id);
                if (lastTerm == null) {
                    lastTerm = Counter.builder(PREFIX + ".partition.terms")
                                      .description("term of partition")
                                      .tag("id", id)
                                      .register(this.registry);
                    lastTerm.increment(version);
                    lastTerms.put(id, lastTerm);
                } else {
                    lastTerm.increment(version - lastTerm.count());
                }
                List<Metapb.Shard> shards = g.getShardsList();
                for (Metapb.Shard shard : shards) {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        leaders.put(shard.getStoreId(), leaders.getOrDefault(shard.getStoreId(), 0) + 1);
                        break;
                    }
                }
            }
            leaderCounts.clear();
            for (Map.Entry<Long, Integer> entry : leaders.entrySet()) {
                Long storeId = entry.getKey();
                String address = stores.get(storeId).getAddress();
                leaderCounts.put(address, entry.getValue());
            }
        } catch (Exception e) {
            log.info("get partition term with error :", e);
        }
        if (groups == null) {
            return 0;
        } else {
            return groups.size();
        }
    }

    private List<Metapb.Graph> getGraphMetas() {
        try {
            return this.pdService.getPartitionService().getGraphs();
        } catch (PDException e) {
            log.error(e.getMessage(), e);
        }
        return Collections.EMPTY_LIST;
    }

    private void registerGraphMetrics() {
        this.getGraphMetas().forEach(meta -> {
            Gauge.builder(PREFIX + ".partitions", this.pdService.getPartitionService(),
                          e -> e.getPartitions(meta.getGraphName()).size())
                 .description("Number of partitions assigned to a graph")
                 .tag("graph", meta.getGraphName())
                 .register(this.registry);
            ToDoubleFunction<Metapb.Graph> getGraphSize = e -> {
                try {
                    String graphName = e.getGraphName();
                    Pair<Long, Long> last = lasts.get(graphName);
                    Long lastTime;
                    if (last == null || (lastTime = last.getLeft()) == null ||
                        System.currentTimeMillis() - lastTime >= interval) {
                        long dataSize = new GraphStatistics(e, pdRestService, pdService).getDataSize();
                        lasts.put(graphName, Pair.of(System.currentTimeMillis(), dataSize));
                        return dataSize;
                    } else {
                        return last.getRight();
                    }
                } catch (PDException ex) {
                    log.error("get graph size with error", e);
                }
                return 0;
            };
            Gauge.builder(PREFIX + ".graph.size", meta, getGraphSize)
                 .description("data size of graph")
                 .tag("graph", meta.getGraphName())
                 .register(this.registry);
        });
    }
}
