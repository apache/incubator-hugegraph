package org.apache.hugegraph.pd.common;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionCache {

    // 每张图一个缓存
    private volatile Map<String, RangeMap<Long, Integer>> keyToPartIdCache;
    // graphName + PartitionID组成key
    private volatile Map<String, Metapb.Partition> partitionCache;

    private volatile Map<Integer, Metapb.ShardGroup> shardGroupCache;

    private volatile Map<Long, Metapb.Store> storeCache;

    private volatile Map<String, Metapb.Graph> graphCache;
    // 读写锁对象
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    Lock writeLock = readWriteLock.writeLock();

    public PartitionCache() {
        keyToPartIdCache = new HashMap<>();
        partitionCache = new HashMap<>();
        shardGroupCache = new ConcurrentHashMap<>();
        storeCache = new ConcurrentHashMap<>();
        graphCache = new ConcurrentHashMap<>();
    }

    /**
     * 根据partitionId返回分区信息
     *
     * @param graphName
     * @param partId
     * @return
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionById(String graphName, int partId) {
        var partition = partitionCache.get(makePartitionKey(graphName, partId));
        if (partition != null) {
            return new KVPair<>(partition, getLeaderShard(partId));
        }
        return null;
    }

    /**
     * 返回key所在的分区信息
     *
     * @param key
     * @return
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByKey(String graphName, byte[] key) {
        int code = PartitionUtils.calcHashcode(key);
        return getPartitionByCode(graphName, code);
    }

    /**
     * 根据key的hashcode返回分区信息
     *
     * @param graphName
     * @param code
     * @return
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByCode(String graphName, long code) {
        RangeMap<Long, Integer> rangeMap = keyToPartIdCache.get(graphName);
        if (rangeMap != null) {
            Integer partId = rangeMap.get(code);
            if (partId != null) {
                return getPartitionById(graphName, partId);
            }
        }
        return null;
    }

    public List<Metapb.Partition> getPartitions(String graphName) {
        List<Metapb.Partition> partitions = new ArrayList<>();
        // partitionCache key: graph name + partition id
        partitionCache.forEach((k,v) -> {
            if (k.startsWith(graphName)) {
                partitions.add(v);
            }
        });

        return partitions;
    }

    public boolean addPartition(String graphName, int partId, Metapb.Partition partition) {
        writeLock.lock();
        try {
            // graphName + PartitionID组成key
            Metapb.Partition old = partitionCache.get(makePartitionKey(graphName, partId));

            if (old != null && old.equals(partition)) {
                return false;
            }

            Map<String, RangeMap<Long, Integer>> tmpKeyToPartIdCache = cloneKeyToPartIdCache();
            Map<String, Metapb.Partition> tmpPartitionCache = clonePartitionCache();

            tmpPartitionCache.put(makePartitionKey(graphName, partId), partition);
            if (!tmpKeyToPartIdCache.containsKey(graphName)) {
                tmpKeyToPartIdCache.put(graphName, TreeRangeMap.create());
            }

            if (old != null) {
                // old [1-3) 被 [2-3)覆盖了。当 [1-3) 变成[1-2) 不应该删除原先的[1-3)
                // 当确认老的 start, end 都是自己的时候，才可以删除老的. (即还没覆盖）
                var graphRange = tmpKeyToPartIdCache.get(graphName);
                if (Objects.equals(partition.getId(), graphRange.get(partition.getStartKey())) &&
                        Objects.equals(partition.getId(), graphRange.get(partition.getEndKey() - 1))) {
                    graphRange.remove(graphRange.getEntry(partition.getStartKey()).getKey());
                }
            }

            tmpKeyToPartIdCache.get(graphName)
                    .put(Range.closedOpen(partition.getStartKey(), partition.getEndKey()), partId);
            partitionCache = tmpPartitionCache;
            keyToPartIdCache = tmpKeyToPartIdCache;
            return true;
        } finally {
            writeLock.unlock();
        }

    }

    public void updatePartition(String graphName, int partId, Metapb.Partition partition) {
        writeLock.lock();
        try {
            Map<String, RangeMap<Long, Integer>> tmpKeyToPartIdCache = cloneKeyToPartIdCache();
            Map<String, Metapb.Partition> tmpPartitionCache = clonePartitionCache();

            Metapb.Partition old = tmpPartitionCache.get(makePartitionKey(graphName, partId));

            tmpPartitionCache.put(makePartitionKey(graphName, partId), partition);

            if (!tmpKeyToPartIdCache.containsKey(graphName)) {
                tmpKeyToPartIdCache.put(graphName, TreeRangeMap.create());
            }

            if (old != null) {
                var graphRange = tmpKeyToPartIdCache.get(graphName);
                if (Objects.equals(partition.getId(), graphRange.get(partition.getStartKey())) &&
                        Objects.equals(partition.getId(), graphRange.get(partition.getEndKey() - 1))) {
                    graphRange.remove(graphRange.getEntry(partition.getStartKey()).getKey());

                }
            }

            tmpKeyToPartIdCache.get(graphName)
                    .put(Range.closedOpen(partition.getStartKey(), partition.getEndKey()), partId);
            partitionCache = tmpPartitionCache;
            keyToPartIdCache = tmpKeyToPartIdCache;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean updatePartition(Metapb.Partition partition) {

        var graphName = partition.getGraphName();
        var partitionId = partition.getId();

        var old = getPartitionById(graphName, partitionId);
        if (old != null && Objects.equals(partition, old.getKey())) {
            return false;
        }

        updatePartition(graphName, partitionId, partition);
        return true;
    }

    public void removePartition(String graphName, int partId) {
        writeLock.lock();
        try {
            Map<String, RangeMap<Long, Integer>> tmpKeyToPartIdCache = cloneKeyToPartIdCache();
            Map<String, Metapb.Partition> tmpPartitionCache = clonePartitionCache();
            Metapb.Partition partition = tmpPartitionCache.remove(makePartitionKey(graphName, partId));
            if (partition != null) {
                var graphRange = tmpKeyToPartIdCache.get(graphName);

                if (Objects.equals(partition.getId(), graphRange.get(partition.getStartKey())) &&
                        Objects.equals(partition.getId(), graphRange.get(partition.getEndKey() - 1))) {
                    graphRange.remove(graphRange.getEntry(partition.getStartKey()).getKey());
                }

            }
            partitionCache = tmpPartitionCache;
            keyToPartIdCache = tmpKeyToPartIdCache;
            // log.info("PartitionCache.removePartition : (after){}", debugCacheByGraphName(graphName));
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * remove partition id of graph name
     * @param graphName
     * @param id
     */
    public void remove(String graphName, int id) {
        removePartition(graphName, id);
    }

    /**
     * remove all partitions
     */
    public void removePartitions() {
        writeLock.lock();
        try {
            partitionCache = new HashMap<>();
            keyToPartIdCache = new HashMap<>();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * remove partition cache of graphName
     * @param graphName
     */
    public void removeAll(String graphName) {
        writeLock.lock();
        try {
            Map<String, RangeMap<Long, Integer>> tmpKeyToPartIdCache = cloneKeyToPartIdCache();
            Map<String, Metapb.Partition> tmpPartitionCache = clonePartitionCache();
            var itr = tmpPartitionCache.entrySet().iterator();
            while (itr.hasNext()) {
               var entry = itr.next();
               if (entry.getKey().startsWith(graphName)) {
                    itr.remove();
                }
            }
            tmpKeyToPartIdCache.remove(graphName);
            partitionCache = tmpPartitionCache;
            keyToPartIdCache = tmpKeyToPartIdCache;
        } finally {
            writeLock.unlock();
        }
    }

    private String makePartitionKey(String graphName, int partId) {
        return graphName + "/" + partId;
    }

    public boolean updateShardGroup(Metapb.ShardGroup shardGroup){
        Metapb.ShardGroup oldShardGroup = shardGroupCache.get(shardGroup.getId());
        if (oldShardGroup != null && oldShardGroup.equals(shardGroup)){
            return false;
        }
        shardGroupCache.put(shardGroup.getId(), shardGroup);
        return true;
    }

    public void deleteShardGroup(int shardGroupId){
        if (shardGroupCache.containsKey(shardGroupId)) {
            shardGroupCache.remove(shardGroupId);
        }
    }

    public Metapb.ShardGroup getShardGroup(int groupId){
        return shardGroupCache.get(groupId);
    }

    public boolean addStore(Long storeId, Metapb.Store store) {
        Metapb.Store oldStore = storeCache.get(storeId);
        if (oldStore != null && oldStore.equals(store)) {
            return false;
        }
        storeCache.put(storeId, store);
        return true;
    }

    public Metapb.Store getStoreById(Long storeId) {
        return storeCache.get(storeId);
    }

    public void removeStore(Long storeId) {
        storeCache.remove(storeId);
    }

    public boolean hasGraph(String graphName) {
        return getPartitions(graphName).size() > 0;
    }

    public void updateGraph(Metapb.Graph graph) {
        if (Objects.equals(graph, getGraph(graph.getGraphName()))) {
            return;
        }
        graphCache.put(graph.getGraphName(), graph);
    }

    public Metapb.Graph getGraph(String graphName) {
        return graphCache.get(graphName);
    }

    public List<Metapb.Graph> getGraphs() {
        List<Metapb.Graph> graphs = new ArrayList<>();
        graphCache.forEach((k, v) -> {
            graphs.add(v);
        });
        return graphs;
    }

    private Map<String, RangeMap<Long, Integer>> cloneKeyToPartIdCache() {
        Map<String, RangeMap<Long, Integer>> cacheClone = new HashMap<>();
        keyToPartIdCache.forEach((k1, v1) -> {
            cacheClone.put(k1, TreeRangeMap.create());
            v1.asMapOfRanges().forEach((k2, v2) -> {
                cacheClone.get(k1).put(k2, v2);
            });
        });
        return cacheClone;
    }

    private Map<String, Metapb.Partition> clonePartitionCache() {
        Map<String, Metapb.Partition> cacheClone = new HashMap<>();
        cacheClone.putAll(partitionCache);
        return cacheClone;
    }

    public void reset() {
        writeLock.lock();
        try {
            partitionCache = new HashMap<>();
            keyToPartIdCache = new HashMap<>();
            shardGroupCache = new ConcurrentHashMap<>();
            storeCache = new ConcurrentHashMap<>();
            graphCache = new ConcurrentHashMap<>();
        } finally {
            writeLock.unlock();
        }
    }

    public void clear(){
        reset();
    }

    public String debugCacheByGraphName(String graphName) {
        StringBuilder builder = new StringBuilder();
        builder.append("Graph:").append(graphName).append(", cache info: range info: {");
        var rangeMap = keyToPartIdCache.get(graphName);
        builder.append( rangeMap == null ? "" : rangeMap).append("}");

        if (rangeMap != null) {
            builder.append(", partition info : {");
            rangeMap.asMapOfRanges().forEach((k, v) -> {
                var partition = partitionCache.get(makePartitionKey(graphName, v));
                builder.append("[part_id:").append(v);
                if (partition != null) {
                    builder.append(", start_key:").append(partition.getStartKey())
                            .append(", end_key:").append(partition.getEndKey())
                            .append(", state:").append(partition.getState().name());
                }
                builder.append("], ");
            });
            builder.append("}");
        }

        builder.append(", graph info:{");
        var graph =  graphCache.get(graphName);
        if (graph != null) {
            builder.append("partition_count:").append(graph.getPartitionCount())
                    .append(", state:").append(graph.getState().name());
        }
        builder.append("}]");
        return builder.toString();
    }

    public Metapb.Shard getLeaderShard(int partitionId){
        var shardGroup = shardGroupCache.get(partitionId);
        if (shardGroup != null) {
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    return shard;
                }
            }
        }

        return null;
    }

    public void updateShardGroupLeader(int partitionId, Metapb.Shard leader){
        if (shardGroupCache.containsKey(partitionId) && leader != null) {
            if (! Objects.equals(getLeaderShard(partitionId), leader)) {
                var shardGroup = shardGroupCache.get(partitionId);
                var builder = Metapb.ShardGroup.newBuilder(shardGroup).clearShards();
                for (var shard : shardGroup.getShardsList()) {
                    builder.addShards(
                            Metapb.Shard.newBuilder()
                                    .setStoreId(shard.getStoreId())
                                    .setRole(shard.getStoreId() == leader.getStoreId() ?
                                            Metapb.ShardRole.Leader : Metapb.ShardRole.Follower)
                                    .build()
                    );
                }
                shardGroupCache.put(partitionId, builder.build());
            }
        }
    }

    public String debugShardGroup(){
        StringBuilder builder = new StringBuilder();
        builder.append("shard group cache:{");
        shardGroupCache.forEach((partitionId,shardGroup) ->{
            builder.append(partitionId).append("::{")
                    .append("version:").append(shardGroup.getVersion())
                    .append(", conf_version:").append(shardGroup.getConfVer())
                    .append(", state:").append(shardGroup.getState().name())
                    .append(", shards:[");

                for (var shard : shardGroup.getShardsList()) {
                    builder.append("{store_id:").append(shard.getStoreId())
                            .append(", role:").append(shard.getRole().name())
                            .append("},");
                }
                builder.append("], ");
        });
        builder.append("}");
        return builder.toString();
    }
}
