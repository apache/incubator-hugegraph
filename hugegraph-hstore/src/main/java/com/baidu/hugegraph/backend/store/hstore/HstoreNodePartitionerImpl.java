package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.common.PartitionUtils;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.client.*;
import com.baidu.hugegraph.store.client.type.HgNodeStatus;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.store.term.HgPair;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;
import com.baidu.hugegraph.util.Log;
import org.junit.Assert;
import org.slf4j.Logger;

public class HstoreNodePartitionerImpl implements HgStoreNodePartitioner,
                                                  HgStoreNodeProvider,
                                                  HgStoreNodeNotifier {

    private static final Logger LOG = Log.logger(HstoreNodePartitionerImpl.class);
    private PDClient pdClient;
    private HgStoreNodeManager nodeManager;

    protected  HstoreNodePartitionerImpl(){

    }
    public HstoreNodePartitionerImpl(String pdPeers) {
        pdClient = HstoreSessionsImpl.getDefaultPdClient();
    }

    public HstoreNodePartitionerImpl(HgStoreNodeManager nodeManager,
                                     String pdPeers) {
        this(pdPeers);
        this.nodeManager = nodeManager;
    }

    public void setPDClient(PDClient pdClient){
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
                for (Metapb.Store store:stores) {
                    partitions.add(HgNodePartition.of(store.getId(), -1));
                }

            } else if (endKey == HgStoreClientConst.EMPTY_BYTES
                    || startKey == endKey || Arrays.equals(startKey, endKey)){
                HgPair<Metapb.Partition, Metapb.Shard> partShard =
                       pdClient.getPartition(graphName, startKey);
                Metapb.Shard leader = partShard.getValue();
                partitions = new HashSet<>(1);
                partitions.add(HgNodePartition.of(leader.getStoreId(),
                                                  pdClient.keyToCode(graphName, startKey)));
            } else {
                LOG.warn("StartOwnerkey is not equal to endOwnerkey, which is meaningless!!, It is a error!!");
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                for (Metapb.Store store:stores) {
                    partitions.add(HgNodePartition.of(store.getId(), -1));
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
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
            while (partition == null || partition.getEndKey() < endKey){
                HgPair<Metapb.Partition, Metapb.Shard> partShard =
                        pdClient.getPartitionByCode(graphName, startKey);
                if (partShard != null){
                    partition = partShard.getKey();
                    Metapb.Shard leader = partShard.getValue();
                    partitions.add(HgNodePartition.of(leader.getStoreId(), startKey));
                    startKey = (int) partition.getEndKey();
                } else {
                    break;
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    /**
     * 查询hgstore信息
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
                LOG.warn("updatePartitionLeader:{}-{}-{} ",graphName, partId, leader);
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
                HgNodeStatus.NOT_PARTITION_LEADER)){
            pdClient.invalidPartitionCache();
            LOG.warn("invalidPartitionCache:{} ",storeNotice.getNodeStatus());
        }
        return 0;
    }

    public Metapb.GraphWorkMode setWorkMode(String graphName,
                                            Metapb.GraphWorkMode mode) {
        try {
            Metapb.Graph graph = pdClient.setGraph(Metapb.Graph.newBuilder()
                    .setGraphName(graphName)
                    .setWorkMode(mode).build());
            return graph.getWorkMode();
        } catch (PDException e) {
            throw new BackendException("Error while calling pd method, cause:",
                    e.getMessage());
        }
    }
    public void setNodeManager(HgStoreNodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }
}


class FakeHstoreNodePartitionerImpl extends HstoreNodePartitionerImpl {
    private String hstorePeers;
    HgStoreNodeManager nodeManager;
    private static final Logger LOG = Log.logger(HstoreNodePartitionerImpl.class);
    private static int partitionCount = 3;
    private static Map<Integer, Long> leaderMap = new ConcurrentHashMap<>();
    private static Map<Long, String> storeMap = new ConcurrentHashMap<>();

    public FakeHstoreNodePartitionerImpl(String pdPeers) {
        this.hstorePeers = pdPeers;
        // store列表
        for (String address : hstorePeers.split(",")) {
            storeMap.put((long) address.hashCode(), address);
        }
        // 分区列表
        for (int i = 0; i < partitionCount; i++)
            leaderMap.put(i, (long) storeMap.keySet().iterator().next());
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
        int endCode = PartitionUtils.calcHashcode(endKey);
        HashSet<HgNodePartition> partitions = new HashSet<>(storeMap.size());
        if (ALL_PARTITION_OWNER == startKey) {
            storeMap.forEach((k,v)->{
                partitions.add(HgNodePartition.of(k, -1));
            });
        } else if (endKey == HgStoreClientConst.EMPTY_BYTES || startKey == endKey || Arrays.equals(startKey, endKey)) {
            partitions.add(HgNodePartition.of(leaderMap.get(startCode % partitionCount), startCode));
        } else {
            LOG.error("OwnerKey转成HashCode后已经无序了， 按照OwnerKey范围查询没意义");
            storeMap.forEach((k,v)->{
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
             HugeConfig config, HgStoreNodeManager nodeManager){
          if ( config.get(HstoreOptions.PD_FAKE) ) // 无PD模式
              return new FakeHstoreNodePartitionerImpl(nodeManager,
                     config.get(HstoreOptions.HSTORE_PEERS));
          else
              return new HstoreNodePartitionerImpl(nodeManager,
                         config.get(HstoreOptions.PD_PEERS)
              );
        }
    }

}