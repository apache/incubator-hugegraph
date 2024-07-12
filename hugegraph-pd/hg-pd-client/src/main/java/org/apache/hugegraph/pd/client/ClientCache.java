/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.pd.common.GraphCache;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import org.apache.hugegraph.pd.grpc.Metapb.Shard;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.Pdpb.CachePartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientCache {

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final org.apache.hugegraph.pd.client.PDClient client;
    private volatile Map<Integer, KVPair<ShardGroup, Shard>> groups;
    private volatile Map<Long, Metapb.Store> stores;
    private volatile Map<String, GraphCache> caches = new ConcurrentHashMap<>();

    public ClientCache(org.apache.hugegraph.pd.client.PDClient pdClient) {
        groups = new ConcurrentHashMap<>();
        stores = new ConcurrentHashMap<>();
        client = pdClient;
    }

    private GraphCache getGraphCache(String graphName) {
        GraphCache graph;
        if ((graph = caches.get(graphName)) == null) {
            synchronized (caches) {
                if ((graph = caches.get(graphName)) == null) {
                    graph = new GraphCache();
                    caches.put(graphName, graph);
                }
            }
        }
        return graph;
    }

    public KVPair<Partition, Shard> getPartitionById(String graphName, int partId) {
        try {
            GraphCache graph = initGraph(graphName);
            Partition partition = graph.getPartition(partId);
            if (partition == null) {
                return null;
            }
            KVPair<ShardGroup, Shard> group = groups.get(partId);
            if (group == null) {
                return null;
            }
            Shard shard = group.getValue();
            if (shard == null) {
                return null;
            }
            return new KVPair<>(partition, shard);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KVPair<Partition, Shard> getPair(int partId, GraphCache graph) {
        Partition p = graph.getPartition(partId);
        KVPair<ShardGroup, Shard> pair = groups.get(partId);
        if (p != null && pair != null) {
            Shard s = pair.getValue();
            if (s == null) {
                pair.setValue(getLeader(partId));
                return new KVPair<>(p, pair.getValue());
            } else {
                return new KVPair<>(p, s);
            }
        }
        return null;
    }

    /**
     * 根据key的hashcode返回分区信息
     *
     * @param graphName
     * @param code
     * @return
     */
    public KVPair<Partition, Shard> getPartitionByCode(String graphName, long code) {
        try {
            GraphCache graph = initGraph(graphName);
            RangeMap<Long, Integer> range = graph.getRange();
            Integer pId = range.get(code);
            if (pId != null) {
                return getPair(pId, graph);
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
                    CachePartitionResponse pc = client.getPartitionCache(graphName);
                    RangeMap<Long, Integer> range = graph.getRange();
                    List<Partition> ps = pc.getPartitionsList();
                    HashMap<Integer, Partition> gps = new HashMap<>(ps.size(), 1);
                    for (Partition p : ps) {
                        gps.put(p.getId(), p);
                        range.put(Range.closedOpen(p.getStartKey(), p.getEndKey()), p.getId());
                    }
                    graph.setPartitions(gps);
                    graph.getInitialized().set(true);
                }
            }
        }
        return graph;
    }

    private void initCache() throws PDException {
        if (!initialized.get()) {
            synchronized (this) {
                if (!initialized.get()) {
                    CacheResponse cache = client.getClientCache();
                    List<ShardGroup> shardGroups = cache.getShardsList();
                    for (ShardGroup s : shardGroups) {
                        this.groups.put(s.getId(), new KVPair<>(s, getLeader(s.getId())));
                    }
                    List<Metapb.Store> stores = cache.getStoresList();
                    for (Metapb.Store store : stores) {
                        this.stores.put(store.getId(), store);
                    }
                    List<Metapb.Graph> graphs = cache.getGraphsList();
                    for (Metapb.Graph g : graphs) {
                        GraphCache c = new GraphCache(g);
                        caches.put(g.getGraphName(), c);
                    }
                    initialized.set(true);
                }
            }
        }
    }

    /**
     * 返回key所在的分区信息
     *
     * @param key
     * @return
     */
    public KVPair<Partition, Shard> getPartitionByKey(String graphName, byte[] key) {
        int code = PartitionUtils.calcHashcode(key);
        return getPartitionByCode(graphName, code);
    }

    public boolean update(String graphName, int partId, Partition partition) {
        GraphCache graph = getGraphCache(graphName);
        try {
            Partition p = graph.getPartition(partId);
            if (p != null && p.equals(partition)) {
                return false;
            }
            RangeMap<Long, Integer> range = graph.getRange();
            graph.addPartition(partId, partition);
            if (p != null) {
                // old [1-3) 被 [2-3)覆盖了。当 [1-3) 变成[1-2) 不应该删除原先的[1-3)
                // 当确认老的 start, end 都是自己的时候，才可以删除老的. (即还没覆盖）
                if (Objects.equals(partition.getId(), range.get(partition.getStartKey())) &&
                    Objects.equals(partition.getId(), range.get(partition.getEndKey() - 1))) {
                    range.remove(range.getEntry(partition.getStartKey()).getKey());
                }
            }
            range.put(Range.closedOpen(partition.getStartKey(), partition.getEndKey()), partId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public void removePartition(String graphName, int partId) {
        GraphCache graph = getGraphCache(graphName);
        Partition p = graph.removePartition(partId);
        if (p != null) {
            RangeMap<Long, Integer> range = graph.getRange();
            if (Objects.equals(p.getId(), range.get(p.getStartKey())) &&
                Objects.equals(p.getId(), range.get(p.getEndKey() - 1))) {
                range.remove(range.getEntry(p.getStartKey()).getKey());
            }
        }
    }

    /**
     * remove all partitions
     */
    public void removePartitions() {
        for (Entry<String, GraphCache> entry : caches.entrySet()) {
            removePartitions(entry.getValue());
        }
    }

    private void removePartitions(GraphCache graph) {
        graph.getState().clear();
        graph.getRange().clear();
    }

    /**
     * remove partition cache of graphName
     *
     * @param graphName
     */
    public void removeAll(String graphName) {
        GraphCache graph = caches.get(graphName);
        if (graph != null) {
            removePartitions(graph);
        }
    }

    public boolean updateShardGroup(ShardGroup shardGroup) {
        KVPair<ShardGroup, Shard> old = groups.get(shardGroup.getId());
        Shard leader = getLeader(shardGroup);
        if (old != null) {
            old.setKey(shardGroup);
            old.setValue(leader);
            return false;
        }
        groups.put(shardGroup.getId(), new KVPair<>(shardGroup, leader));
        return true;
    }

    public void deleteShardGroup(int shardGroupId) {
        groups.remove(shardGroupId);
    }

    public ShardGroup getShardGroup(int groupId) {
        KVPair<ShardGroup, Shard> pair = groups.get(groupId);
        if (pair != null) {
            return pair.getKey();
        }
        return null;
    }

    public boolean addStore(Long storeId, Metapb.Store store) {
        Metapb.Store oldStore = stores.get(storeId);
        if (oldStore != null && oldStore.equals(store)) {
            return false;
        }
        stores.put(storeId, store);
        return true;
    }

    public Metapb.Store getStoreById(Long storeId) {
        return stores.get(storeId);
    }

    public void removeStore(Long storeId) {
        stores.remove(storeId);
    }

    public void reset() {
        groups = new ConcurrentHashMap<>();
        stores = new ConcurrentHashMap<>();
        caches = new ConcurrentHashMap<>();
        initialized.set(false);
    }

    public Shard getLeader(int partitionId) {
        KVPair<ShardGroup, Shard> pair = groups.get(partitionId);
        if (pair != null) {
            if (pair.getValue() != null) {
                return pair.getValue();
            }
            for (Shard shard : pair.getKey().getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    pair.setValue(shard);
                    return shard;
                }
            }
        }

        return null;
    }

    public Shard getLeader(ShardGroup shardGroup) {
        if (shardGroup != null) {
            for (Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    return shard;
                }
            }
        }

        return null;
    }

    public void updateLeader(int partitionId, Shard leader) {
        KVPair<ShardGroup, Shard> pair = groups.get(partitionId);
        if (pair != null && leader != null) {
            Shard l = getLeader(partitionId);
            if (l == null || leader.getStoreId() != l.getStoreId()) {
                ShardGroup shardGroup = pair.getKey();
                ShardGroup.Builder builder = ShardGroup.newBuilder(shardGroup).clearShards();
                for (var shard : shardGroup.getShardsList()) {
                    builder.addShards(
                            Shard.newBuilder()
                                 .setStoreId(shard.getStoreId())
                                 .setRole(shard.getStoreId() == leader.getStoreId() ?
                                          Metapb.ShardRole.Leader : Metapb.ShardRole.Follower)
                                 .build()
                    );
                }
                pair.setKey(builder.build());
                pair.setValue(leader);
            }
        }
    }
}
