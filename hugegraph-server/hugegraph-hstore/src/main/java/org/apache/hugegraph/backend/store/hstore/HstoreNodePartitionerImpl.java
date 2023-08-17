/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.client.HgNodePartition;
import org.apache.hugegraph.store.client.HgNodePartitionerBuilder;
import org.apache.hugegraph.store.client.HgStoreNode;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.HgStoreNodeNotifier;
import org.apache.hugegraph.store.client.HgStoreNodePartitioner;
import org.apache.hugegraph.store.client.HgStoreNodeProvider;
import org.apache.hugegraph.store.client.HgStoreNotice;
import org.apache.hugegraph.store.client.type.HgNodeStatus;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class HstoreNodePartitionerImpl implements HgStoreNodePartitioner,
                                                  HgStoreNodeProvider,
                                                  HgStoreNodeNotifier {

    private static final Logger LOG = Log.logger(HstoreNodePartitionerImpl.class);
    private PDClient pdClient;
    private HgStoreNodeManager nodeManager;

    protected HstoreNodePartitionerImpl() {

    }

    public HstoreNodePartitionerImpl(String pdPeers) {
        pdClient = HstoreSessionsImpl.getDefaultPdClient();
    }

    public HstoreNodePartitionerImpl(HgStoreNodeManager nodeManager,
                                     String pdPeers) {
        this(pdPeers);
        this.nodeManager = nodeManager;
    }

    public void setPDClient(PDClient pdClient) {
        this.pdClient = pdClient;
    }

    /**
     * 查询分区信息，结果通过HgNodePartitionerBuilder返回
     */
    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         byte[] startKey, byte[] endKey) {
        try {
            HashSet<HgNodePartition> partitions = null;
            if (HgStoreClientConst.ALL_PARTITION_OWNER == startKey) {
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                partitions = new HashSet<>(stores.size());
                for (Metapb.Store store : stores) {
                    partitions.add(HgNodePartition.of(store.getId(), -1));
                }

            } else if (endKey == HgStoreClientConst.EMPTY_BYTES
                       || startKey == endKey || Arrays.equals(startKey, endKey)) {
                KVPair<Metapb.Partition, Metapb.Shard> partShard =
                        pdClient.getPartition(graphName, startKey);
                Metapb.Shard leader = partShard.getValue();
                partitions = new HashSet<>(1);
                partitions.add(HgNodePartition.of(leader.getStoreId(),
                                                  pdClient.keyToCode(graphName, startKey)));
            } else {
                LOG.warn(
                        "StartOwnerkey is not equal to endOwnerkey, which is meaningless!!, It is" +
                        " a error!!");
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                for (Metapb.Store store : stores) {
                    partitions.add(HgNodePartition.of(store.getId(), -1));
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            LOG.error("An error occurred while getting partition information :{}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         int startKey, int endKey) {
        try {
            HashSet<HgNodePartition> partitions = new HashSet<>();
            Metapb.Partition partition = null;
            while ((partition == null || partition.getEndKey() < endKey)
                   && startKey < PartitionUtils.MAX_VALUE) {
                KVPair<Metapb.Partition, Metapb.Shard> partShard =
                        pdClient.getPartitionByCode(graphName, startKey);
                if (partShard != null) {
                    partition = partShard.getKey();
                    Metapb.Shard leader = partShard.getValue();
                    partitions.add(HgNodePartition.of(leader.getStoreId(), startKey,
                                                      (int) partition.getStartKey(),
                                                      (int) partition.getEndKey()));
                    startKey = (int) partition.getEndKey();
                } else {
                    break;
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            LOG.error("An error occurred while getting partition information :{}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;

    }

    /**
     * 查询hgstore信息
     *
     * @return hgstore
     */
    @Override
    public HgStoreNode apply(String graphName, Long nodeId) {
        try {
            Metapb.Store store = pdClient.getStore(nodeId);
            return nodeManager.getNodeBuilder().setNodeId(store.getId())
                              .setAddress(store.getAddress()).build();
        } catch (PDException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 通知更新缓存
     */
    @Override
    public int notice(String graphName, HgStoreNotice storeNotice) {
        LOG.warn(storeNotice.toString());
        if (storeNotice.getPartitionLeaders() != null) {
            storeNotice.getPartitionLeaders().forEach((partId, leader) -> {
                pdClient.updatePartitionLeader(graphName, partId, leader);
                LOG.warn("updatePartitionLeader:{}-{}-{}",
                         graphName, partId, leader);
            });
        }
        if (storeNotice.getPartitionIds() != null) {
            storeNotice.getPartitionIds().forEach(partId -> {
                pdClient.invalidPartitionCache(graphName, partId);
            });
        }
        if (!storeNotice.getNodeStatus().equals(
                HgNodeStatus.PARTITION_COMMON_FAULT)
            && !storeNotice.getNodeStatus().equals(
                HgNodeStatus.NOT_PARTITION_LEADER)) {
            pdClient.invalidPartitionCache();
            LOG.warn("invalidPartitionCache:{} ", storeNotice.getNodeStatus());
        }
        return 0;
    }

    public Metapb.Graph delGraph(String graphName) {
        try {
            return pdClient.delGraph(graphName);
        } catch (PDException e) {
            LOG.error("delGraph {} exception, {}", graphName, e.getMessage());
        }
        return null;
    }

    public void setNodeManager(HgStoreNodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }
}

class FakeHstoreNodePartitionerImpl extends HstoreNodePartitionerImpl {
    private static final Logger LOG = Log.logger(HstoreNodePartitionerImpl.class);
    private static final int partitionCount = 3;
    private static final Map<Integer, Long> leaderMap = new ConcurrentHashMap<>();
    private static final Map<Long, String> storeMap = new ConcurrentHashMap<>();
    HgStoreNodeManager nodeManager;
    private final String hstorePeers;

    public FakeHstoreNodePartitionerImpl(String pdPeers) {
        this.hstorePeers = pdPeers;
        // store列表
        for (String address : hstorePeers.split(",")) {
            storeMap.put((long) address.hashCode(), address);
        }
        // 分区列表
        for (int i = 0; i < partitionCount; i++) {
            leaderMap.put(i, storeMap.keySet().iterator().next());
        }
    }

    public FakeHstoreNodePartitionerImpl(HgStoreNodeManager nodeManager,
                                         String peers) {
        this(peers);
        this.nodeManager = nodeManager;
    }

    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         byte[] startKey, byte[] endKey) {
        int startCode = PartitionUtils.calcHashcode(startKey);
        HashSet<HgNodePartition> partitions = new HashSet<>(storeMap.size());
        if (ALL_PARTITION_OWNER == startKey) {
            storeMap.forEach((k, v) -> {
                partitions.add(HgNodePartition.of(k, -1));
            });
        } else if (endKey == HgStoreClientConst.EMPTY_BYTES || startKey == endKey ||
                   Arrays.equals(startKey, endKey)) {
            partitions.add(
                    HgNodePartition.of(leaderMap.get(startCode % partitionCount), startCode));
        } else {
            LOG.error("OwnerKey转成HashCode后已经无序了， 按照OwnerKey范围查询没意义");
            storeMap.forEach((k, v) -> {
                partitions.add(HgNodePartition.of(k, -1));
            });
        }
        builder.setPartitions(partitions);
        return 0;
    }

    @Override
    public HgStoreNode apply(String graphName, Long nodeId) {
        return nodeManager.getNodeBuilder().setNodeId(nodeId)
                          .setAddress(storeMap.get(nodeId)).build();
    }

    @Override
    public int notice(String graphName, HgStoreNotice storeNotice) {
        if (storeNotice.getPartitionLeaders() != null
            && storeNotice.getPartitionLeaders().size() > 0) {
            leaderMap.putAll(storeNotice.getPartitionLeaders());
        }
        return 0;
    }

    public static class NodePartitionerFactory {
        public static HstoreNodePartitionerImpl getNodePartitioner(
                HugeConfig config, HgStoreNodeManager nodeManager) {
            if (config.get(HstoreOptions.PD_FAKE)) {
                return new FakeHstoreNodePartitionerImpl(nodeManager,
                                                         config.get(HstoreOptions.HSTORE_PEERS));
            } else {
                return new HstoreNodePartitionerImpl(nodeManager,
                                                     config.get(HstoreOptions.PD_PEERS)
                );
            }

        }
    }
}
