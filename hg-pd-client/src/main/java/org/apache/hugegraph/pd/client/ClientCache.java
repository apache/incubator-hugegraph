package org.apache.hugegraph.pd.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.client.impl.PDApi;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;

import org.apache.hugegraph.pd.client.rpc.ConnectionClient;
import org.apache.hugegraph.pd.common.GraphCache;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchShardGroupResponse;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;
import org.apache.hugegraph.pd.watch.Watcher;
import com.google.common.collect.RangeMap;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static org.apache.hugegraph.pd.watch.NodeEvent.EventType.NODE_OFFLINE;

@Slf4j
public class ClientCache {

    private final Watcher watcher;
    private volatile Map<Integer, KVPair<Metapb.ShardGroup, Metapb.Shard>> groups;
    private volatile Map<Long, Metapb.Store> stores;
    private volatile Map<String, GraphCache> caches = new ConcurrentHashMap<>();
    private AtomicBoolean initialized = new AtomicBoolean(false);
    @Getter
    private ConnectionClient client;

    @Setter
    private PDApi pdApi;

    public ClientCache(ConnectionClient client, Watcher watcher) {
        this.groups = new ConcurrentHashMap<>();
        this.stores = new ConcurrentHashMap<>();
        this.client = client;
        this.watcher = watcher;
        this.watcher.watchPartition(this::watchPartition);
        this.watcher.watchShardGroup(this::watchShardGroup);
        this.watcher.watchNode(this::watchNode);
    }

    private void watchPartition(PartitionEvent response) {
        invalidPartitionCache(response.getGraph(), response.getPartitionId());
        if (response.getChangeType() == PartitionEvent.ChangeType.DEL) {
            removeAll(response.getGraph());
        }
    }

    private void watchShardGroup(WatchResponse response) {
        WatchShardGroupResponse shardResponse = response.getShardGroupResponse();
        switch (shardResponse.getType()) {
            case WATCH_CHANGE_TYPE_DEL:
                deleteShardGroup(shardResponse.getShardGroupId());
                break;
            case WATCH_CHANGE_TYPE_ALTER:
            case WATCH_CHANGE_TYPE_ADD:
                updateShardGroup(response.getShardGroupResponse().getShardGroup());
                break;
        }
    }

    private void watchNode(NodeEvent response) {
        if (response.getEventType() == NODE_OFFLINE) {
            invalidStoreCache(response.getNodeId());
        } else {
            // update store, 不更新缓存，会造成 getLeaderStoreAddresses的返回结果
            try {
                pdApi.getStore(response.getNodeId());
            } catch (PDException e) {
                log.error("getStore exception", e);
            }
        }
    }

    private void invalidStoreCache(long storeId) {
        removeStore(Long.valueOf(storeId));
    }

    private void invalidPartitionCache(String graphName, int partitionId) {
        if (null != getPartitionById(graphName, partitionId)) {
            removePartition(graphName, partitionId);
        }
    }

    private GraphCache getGraphCache(String graphName) {
        GraphCache graph;
        if ((graph = this.caches.get(graphName)) == null) {
            synchronized (this.caches) {
                if ((graph = this.caches.get(graphName)) == null) {
                    Metapb.Graph.Builder builder = Metapb.Graph.newBuilder().setGraphName(graphName);
                    Metapb.Graph g = builder.build();
                    graph = new GraphCache(g);
                    this.caches.put(graphName, graph);
                }
            }
        }
        return graph;
    }

    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionById(String graphName, int partId) {
        try {
            GraphCache graph = initGraph(graphName);
            Metapb.Partition partition = graph.getPartition(partId);
            if (partition == null || !this.groups.containsKey(partId)) {
                return null;
            }
            Metapb.Shard shard = this.groups.get(Integer.valueOf(partId)).getValue();
            if (shard == null) {
                return null;
            }
            return new KVPair(partition, shard);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KVPair<Metapb.Partition, Metapb.Shard> getPair(int partId, GraphCache graph) {
        Metapb.Partition p = graph.getPartition(Integer.valueOf(partId));
        KVPair<Metapb.ShardGroup, Metapb.Shard> pair = this.groups.get(Integer.valueOf(partId));
        if (p != null && pair != null) {
            Metapb.Shard s = pair.getValue();
            if (s == null) {
                pair.setValue(getLeader(partId));
                return new KVPair(p, pair.getValue());
            }
            return new KVPair(p, s);
        }
        return null;
    }

    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByCode(String graphName, long code) {
        try {
            GraphCache graph = initGraph(graphName);
            RangeMap<Long, Integer> range = graph.getRange();
            Integer pId = range.get(Long.valueOf(code));
            if (pId != null) {
                return getPair(pId.intValue(), graph);
            }
            ReentrantReadWriteLock.ReadLock readLock = graph.getLock().readLock();
            try {
                readLock.lock();
                pId = range.get(Long.valueOf(code));
            } catch (Exception e) {
                log.info("get range with error:", e);
            } finally {
                readLock.unlock();
            }
            if (pId == null) {
                ReentrantReadWriteLock.WriteLock writeLock = graph.getLock().writeLock();
                try {
                    writeLock.lock();
                    if ((pId = range.get(Long.valueOf(code))) == null) {
                        graph.reset();
                        initGraph(graph);
                        pId = range.get(Long.valueOf(code));
                    }
                } catch (Exception e) {
                    log.info("reset with error:", e);
                } finally {
                    writeLock.unlock();
                }
            }
            if (pId != null) {
                return getPair(pId.intValue(), graph);
            }
            return null;
        } catch (PDException e) {
            throw new RuntimeException(e);
        }
    }

    private GraphCache initGraph(String graphName) throws PDException {
        initCache();
        GraphCache graph = getGraphCache(graphName);
        if (!graph.getInitialized().get()) {
            synchronized (graph) {
                if (!graph.getInitialized().get()) {
                    initGraph(graph);
                    graph.getInitialized().set(true);
                }
            }
        }
        return graph;
    }

    private void initGraph(GraphCache graph) throws PDException {
        Pdpb.CachePartitionResponse pc = this.client.getPartitionCache(graph.getGraph().getGraphName());
        List<Metapb.Partition> ps = pc.getPartitionsList();
        if (!CollectionUtils.isEmpty(ps)) {
            graph.init(ps);
        }
    }

    private void initCache() throws PDException {
        if (!this.initialized.get()) {
            synchronized (this) {
                if (!this.initialized.get()) {
                    Pdpb.CacheResponse cache = this.client.getClientCache();
                    List<Metapb.ShardGroup> shardGroups = cache.getShardsList();
                    for (Metapb.ShardGroup s : shardGroups) {
                        this.groups.put(Integer.valueOf(s.getId()), new KVPair(s, getLeader(s)));
                    }
                    List<Metapb.Store> stores = cache.getStoresList();
                    for (Metapb.Store store : stores) {
                        this.stores.put(Long.valueOf(store.getId()), store);
                    }
                    List<Metapb.Graph> graphs = cache.getGraphsList();
                    for (Metapb.Graph g : graphs) {
                        GraphCache c = new GraphCache(g);
                        this.caches.put(g.getGraphName(), c);
                    }
                    this.initialized.set(true);
                }
            }
        }
    }

    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByKey(String graphName, byte[] key) {
        int code = PartitionUtils.calcHashcode(key);
        return getPartitionByCode(graphName, code);
    }

    public boolean update(String graphName, int partId, Metapb.Partition partition) {
        GraphCache graph = getGraphCache(graphName);
        return graph.updatePartition(partition);
    }

    public void removePartition(String graphName, int partId) {
        GraphCache graph = getGraphCache(graphName);
        graph.removePartition(Integer.valueOf(partId));
    }

    public void removePartitions() {
        try {
            this.groups.clear();
            this.stores.clear();
            this.caches.clear();
            this.initialized.set(false);
            initCache();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void removePartitions(GraphCache graph) {
        try {
            graph.removePartitions();
            initGraph(graph.getGraph().getGraphName());
        } catch (Exception e) {
            log.warn("remove partitions with error:", e);
        } finally {
        }
    }

    public void removeAll(String graphName) {
        GraphCache graph = this.caches.get(graphName);
        if (graph != null) {
            removePartitions(graph);
        }
    }

    private StringBuffer getStack(StackTraceElement[] stackTrace) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement element = stackTrace[i];
            sb.append(element.toString() + "\n");
        }
        return sb;
    }

    public boolean updateShardGroup(Metapb.ShardGroup shardGroup) {
        KVPair<Metapb.ShardGroup, Metapb.Shard> old = this.groups.get(Integer.valueOf(shardGroup.getId()));
        Metapb.Shard leader = getLeader(shardGroup);
        if (old != null) {
            old.setKey(shardGroup);
            old.setValue(leader);
            return false;
        }
        this.groups.put(Integer.valueOf(shardGroup.getId()), new KVPair(shardGroup, leader));
        return true;
    }

    public void deleteShardGroup(int shardGroupId) {
        this.groups.remove(Integer.valueOf(shardGroupId));
    }

    public Metapb.ShardGroup getShardGroup(int groupId) {
        KVPair<Metapb.ShardGroup, Metapb.Shard> pair = this.groups.get(Integer.valueOf(groupId));
        if (pair != null) {
            return pair.getKey();
        }
        return null;
    }

    public boolean addStore(Long storeId, Metapb.Store store) {
        Metapb.Store oldStore = this.stores.get(storeId);
        if (oldStore != null && oldStore.equals(store)) {
            return false;
        }
        this.stores.put(storeId, store);
        return true;
    }

    public Metapb.Store getStoreById(Long storeId) {
        return this.stores.get(storeId);
    }

    public void removeStore(Long storeId) {
        this.stores.remove(storeId);
    }

    public void reset() {
        this.groups = new ConcurrentHashMap<>();
        this.stores = new ConcurrentHashMap<>();
        this.caches = new ConcurrentHashMap<>();
        this.initialized.set(false);
    }

    public Metapb.Shard getLeader(int partitionId) {
        KVPair<Metapb.ShardGroup, Metapb.Shard> pair = this.groups.get(Integer.valueOf(partitionId));
        if (pair != null) {
            if (pair.getValue() != null) {
                return pair.getValue();
            }
            for (Metapb.Shard shard : pair.getKey().getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    pair.setValue(shard);
                    return shard;
                }
            }
        }
        return null;
    }

    public Metapb.Shard getLeader(Metapb.ShardGroup shardGroup) {
        if (shardGroup != null) {
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    return shard;
                }
            }
        }
        return null;
    }

    public void updateLeader(int partitionId, Metapb.Shard leader) {
        KVPair<Metapb.ShardGroup, Metapb.Shard> pair = this.groups.get(partitionId);
        if (pair != null && leader != null) {
            Metapb.Shard l = pair.getValue();
            if (l == null || leader.getStoreId() != l.getStoreId()) {
                Metapb.ShardGroup shardGroup = pair.getKey();
                synchronized (shardGroup) {
                    l = pair.getValue();
                    if (l == null || leader.getStoreId() != l.getStoreId()) {
                        log.info("Change leader of partition {} from {} to {}", partitionId, l.getStoreId(),
                                 leader.getStoreId());
                        Metapb.ShardGroup.Builder builder =
                                Metapb.ShardGroup.newBuilder(shardGroup).clearShards();
                        for (Metapb.Shard shard : shardGroup.getShardsList()) {
                            builder.addShards(
                                    Metapb.Shard.newBuilder()
                                                .setStoreId(shard.getStoreId())
                                                .setRole((shard.getStoreId() == leader.getStoreId()) ?
                                                         Metapb.ShardRole.Leader : Metapb.ShardRole.Follower)
                                                .build());
                        }
                        pair.setKey(builder.build());
                        pair.setValue(leader);
                    }
                }
            }
        }
    }

    public List<String> getLeaderStoreAddresses() throws PDException {
        initCache();
        Set<Long> storeIds =
                this.groups.values().stream().map(shardGroupShardKVPair -> shardGroupShardKVPair.getValue()
                                                                                                .getStoreId())
                           .collect(Collectors.toSet());
        return this.stores.values().stream()
                          .filter(store -> storeIds.contains(Long.valueOf(store.getId())))
                          .map(Metapb.Store::getAddress)
                          .collect(Collectors.toList());
    }
}
