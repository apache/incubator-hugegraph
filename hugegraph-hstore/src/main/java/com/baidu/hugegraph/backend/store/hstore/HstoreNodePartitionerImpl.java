package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.client.*;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.store.term.HgPair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;

public class HstoreNodePartitionerImpl implements HgStoreNodePartitioner,
                                                  HgStoreNodeProvider,
                                                  HgStoreNodeNotifier {

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
            if (HgStoreClientConst.ALL_PARTITION_OWNER == startKey) {
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                stores.forEach(e -> {
                    builder.add(e.getId(), -1);
                });
            } else if (startKey == endKey) {
                HgPair<Metapb.Partition, Metapb.Shard> partShard =
                       pdClient.getPartition(graphName, startKey);
                Metapb.Shard leader = partShard.getValue();
                builder.add(leader.getStoreId(), partShard.getKey().getId());
            } else {
                pdClient.scanPartitions(graphName, startKey, endKey)
                        .forEach(e -> {
                    builder.add(e.getValue().getStoreId(), e.getKey().getId());
                });
            }
        } catch (PDException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         int startKey, int endKey) {
        try {
            Metapb.Partition partition = null;
            while (partition == null || partition.getEndKey() < endKey){
                HgPair<Metapb.Partition, Metapb.Shard> partShard =
                        pdClient.getPartitionByCode(graphName, startKey);
                if (partShard != null){
                    partition = partShard.getKey();
                    Metapb.Shard leader = partShard.getValue();
                    builder.add(leader.getStoreId(), partShard.getKey().getId());
                    startKey = (int) partition.getEndKey();
                } else {
                    break;
                }
            }
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
        if (storeNotice.getPartitionLeaders() != null) {
            storeNotice.getPartitionLeaders().forEach((partId, leader) -> {
                pdClient.updatePartitionLeader(graphName, partId, leader);
            });
        }
        if (storeNotice.getPartitionIds() != null) {
            storeNotice.getPartitionIds().forEach(partId -> {
                pdClient.invalidPartitionCache(graphName, partId);
            });
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

    private static int partitionCount = 2;
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
        int id1 = Arrays.hashCode(startKey)
                  & Integer.MAX_VALUE % partitionCount;
        int id2 = Arrays.hashCode(endKey)  & Integer.MAX_VALUE % partitionCount;
        if (ALL_PARTITION_OWNER == startKey) {
            storeMap.forEach((k, v) -> {
                builder.add(k, -1);
            });
        } else if (startKey == endKey) {
            builder.add(leaderMap.get(id1), id1);
        } else {
            builder.add(leaderMap.get(id1), id1);
            builder.add(leaderMap.get(id2), id2);
        }
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