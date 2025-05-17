package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class TaskScheduleServiceTest extends PdTestBase{

    TaskScheduleService service;

    @Before
    public void init() throws PDException {
        service = getTaskService();

        var partitionService = getPartitionService();
        var graph = partitionService.getGraph("graph0");
        if (graph == null) {
            partitionService.createGraph("graph0", 0, 0);
        }

        var storeNodeService = getStoreNodeService();
        storeNodeService.updateStoreGroupRelation(1, 0);
        storeNodeService.updateStoreGroupRelation(2, 0);
        storeNodeService.updateStoreGroupRelation(3, 0);
    }

    @Test
    public void testStoreOffline(){

    }

    public void testPatrolStores(){

    }

    public void testPatrolPartitions(){

    }

    public void testBalancePartitionShard(){

    }

    @Test
    public void testBalancePartitionLeader() throws PDException {

        var list = new ArrayList<Metapb.Partition>();
        for (int i = 0; i < 6; i++){
            getStoreNodeService().getStoreInfoMeta().updateShardGroup(genShardGroup(i));
            list.add(genPartition(i));
        }

        getPdConfig().getPartition().setShardCount(3);

        getPartitionService().updatePartition(list);
        var rst = service.balancePartitionLeader(true);
        // assertTrue(rst.size() > 0 );
        // recover
        getPdConfig().getPartition().setShardCount(1);
        getStoreNodeService().getStoreInfoMeta().removeAll();
    }

    public void testSplitPartition(){

    }
    public void testSplitPartition2(){

    }

    public void testCanAllPartitionsMovedOut(){

    }

    private Metapb.ShardGroup genShardGroup(int groupId){
        return Metapb.ShardGroup.newBuilder()
                .setId(groupId)
                .addAllShards(genShards())
                .build();
    }

    private Metapb.Partition genPartition(int groupId){
        return Metapb.Partition.newBuilder()
                .setId(groupId)
                .setState(Metapb.PartitionState.PState_Normal)
                .setGraphName("graph0")
                .setStartKey(groupId * 10)
                .setEndKey(groupId * 10 + 10)
                .build();
    }

    private List<Metapb.Shard> genShards(){
        return List.of(Metapb.Shard.newBuilder().setStoreId(1).setRole(Metapb.ShardRole.Leader).build(),
                Metapb.Shard.newBuilder().setStoreId(2).setRole(Metapb.ShardRole.Follower).build(),
                Metapb.Shard.newBuilder().setStoreId(3).setRole(Metapb.ShardRole.Follower).build());
    }

}


