package com.baidu.hugegraph.store.pd;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.MetaTask;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.meta.GraphManager;
import com.baidu.hugegraph.store.meta.ShardGroup;
import com.baidu.hugegraph.store.util.HgStoreException;
import com.baidu.hugegraph.store.meta.Store;
import com.baidu.hugegraph.store.meta.Partition;

import java.util.List;
import java.util.function.Consumer;

public interface PdProvider {
    long registerStore(Store store) throws PDException;
    Store getStoreByID(Long storeId);
    Metapb.ClusterStats getClusterStats();

    Metapb.ClusterStats storeHeartbeat(Store node) throws HgStoreException, PDException;
    Partition getPartitionByID(String graph, int partId);
    Metapb.Shard getPartitionLeader(String graph, int partId);
    Metapb.Partition getPartitionByCode(String graph, int code);

    Partition delPartition(String graph, int partId);
    List<Metapb.Partition> updatePartition(List<Metapb.Partition>  partitions) throws PDException;
    List<Partition> getPartitionsByStore(long storeId) throws PDException;

    void updatePartitionCache(Partition partition, Boolean changeLeader);
    void invalidPartitionCache(String graph, int partId);

    boolean startHeartbeatStream(Consumer<Throwable> onError);

    boolean addPartitionInstructionListener(PartitionInstructionListener listener);

    boolean partitionHeartbeat(List<Metapb.PartitionStats> statsList);


    boolean isLocalPartition(long storeId, int partitionId);
    Metapb.Graph getGraph(String graphName) throws PDException;

    void reportTask(MetaTask.Task task) throws PDException;

    PDClient getPDClient();
    boolean updatePartitionLeader(String graphName, int partId, long leaderStoreId);

    GraphManager getGraphManager();
    void setGraphManager(GraphManager graphManager);

    /**
     * 删除分区 shard group
     * @param groupId
     */
    void deleteShardGroup(int groupId) throws PDException;
    default Metapb.ShardGroup getShardGroup(int partitionId){
        return null;
    }

    default void updateShardGroup(Metapb.ShardGroup shardGroup) throws PDException { }

}
