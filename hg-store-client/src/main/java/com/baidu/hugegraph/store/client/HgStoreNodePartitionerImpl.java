package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.common.KVPair;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.common.PartitionUtils;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.client.type.HgNodeStatus;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class HgStoreNodePartitionerImpl implements HgStoreNodePartitioner,
        HgStoreNodeProvider,
        HgStoreNodeNotifier {
    private PDClient pdClient;
    private HgStoreNodeManager nodeManager;

    protected HgStoreNodePartitionerImpl() {
    }

    public HgStoreNodePartitionerImpl(PDClient pdClient, HgStoreNodeManager nodeManager) {
        this.pdClient = pdClient;
        this.nodeManager = nodeManager;
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
                partitions = new HashSet<>();
                partitions.add(HgNodePartition.of(leader.getStoreId(),
                        pdClient.keyToCode(graphName, startKey)));
            } else {
                log.warn("StartOwnerkey is not equal to endOwnerkey, which is meaningless!!, It is a error!!");
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                partitions = new HashSet<>(stores.size());
                for (Metapb.Store store : stores) {
                    partitions.add(HgNodePartition.of(store.getId(), -1));
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            log.error("An error occurred while getting partition information :{}", e.getMessage());
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
                            (int) partition.getStartKey(), (int) partition.getEndKey()));
                    startKey = (int) partition.getEndKey();
                } else {
                    break;
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            log.error("An error occurred while getting partition information :{}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }


    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         int partitionId) {
        try {
            HashSet<HgNodePartition> partitions = new HashSet<>();
            Metapb.Partition partition = null;

            KVPair<Metapb.Partition, Metapb.Shard> partShard =
                    pdClient.getPartitionById(graphName, partitionId);
            if (partShard != null) {
                partition = partShard.getKey();
                Metapb.Shard leader = partShard.getValue();
                partitions.add(HgNodePartition.of(leader.getStoreId(), (int) partition.getStartKey()));
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            log.error("An error occurred while getting partition information :{}", e.getMessage());
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
        log.warn(storeNotice.toString());
        if (storeNotice.getPartitionLeaders() != null) {
            storeNotice.getPartitionLeaders().forEach((partId, leader) -> {
                pdClient.updatePartitionLeader(graphName, partId, leader);
                log.warn("updatePartitionLeader:{}-{}-{}",
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
            log.warn("invalidPartitionCache:{} ", storeNotice.getNodeStatus());
        }
        return 0;
    }

    public Metapb.Graph delGraph(String graphName) {
        try {
            return pdClient.delGraph(graphName);
        } catch (PDException e) {
            log.error("delGraph {} exception, {}", graphName, e.getMessage());
        }
        return null;
    }

    public void setNodeManager(HgStoreNodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }
}

