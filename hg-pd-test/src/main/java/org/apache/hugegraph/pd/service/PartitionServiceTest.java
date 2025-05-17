package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;
import static org.junit.Assert.assertEquals;

public class PartitionServiceTest extends PdTestBase {

    private PartitionService service;

    @Before
    public void init(){
        service = getPartitionService();
    }

    @Test
    public void testCombinePartition() throws PDException {
        buildEnv();
        // 0, 1, 2-> 0, 3,4,5->1, 6,7,8 ->2, 9,10, 11-> 3
        service.combinePartition(DEFAULT_STORE_GROUP_ID, 4);

        var partition = service.getPartitionById("graph0", 0);
        assertEquals(0, partition.getStartKey());
        assertEquals(5462, partition.getEndKey());

        var tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(11, tasks.size());

        for (MetaTask.Task task : tasks){
            var newTask = task.toBuilder().setState(MetaTask.TaskState.Task_Success).build();
            getTaskService().reportTask(newTask);
        }

        tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(0, tasks.size());
    }

    @Test
    public void testCombinePartition2() throws PDException {
        buildEnv();
        // 0, 1, 2-> 0, 3,4,5->1, 6,7,8 ->2, 9,10, 11-> 3
        service.combinePartition(DEFAULT_STORE_GROUP_ID, 4);

        var partition = service.getPartitionById("graph0", 0);
        assertEquals(0, partition.getStartKey());
        assertEquals(5462, partition.getEndKey());

        var tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(11, tasks.size());

        for (MetaTask.Task task : tasks){
            var newTask = task.toBuilder().setState(MetaTask.TaskState.Task_Failure).build();
            getTaskService().reportTask(newTask);
        }

        tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(0, tasks.size());
    }

    @Test
    public void testHandleCleanTask(){
        MetaTask.Task task = MetaTask.Task.newBuilder()
                .setType(MetaTask.TaskType.Clean_Partition)
                .setPartition(Metapb.Partition.newBuilder().setGraphName("foo").setId(0).build())
                .setCleanPartition(CleanPartition.newBuilder()
                        .setCleanType(CleanType.CLEAN_TYPE_KEEP_RANGE)
                        .setDeletePartition(true)
                        .setKeyStart(0)
                        .setKeyEnd(10)
                        .build())
                .build();
        getTaskService().reportTask(task);
    }

    private void buildEnv() throws PDException {
        var graph = service.getGraph("graph0");
        if (graph == null) {
            service.createGraph("graph0", 0, 0);
        }

        var storeInfoMeta = getStoreNodeService().getStoreInfoMeta();
        storeInfoMeta.updateStore(Metapb.Store.newBuilder()
                .setId(99)
                .setState(Metapb.StoreState.Up)
                .build());

        getStoreNodeService().updateStoreGroupRelation(99, 0);

        long lastId = 0;
        for (int i = 0; i < 12; i++){
            Metapb.Shard shard = Metapb.Shard.newBuilder()
                    .setStoreId(99)
                    .setRole(Metapb.ShardRole.Leader)
                    .build();

            Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                    .setId(i)
                    .setState(Metapb.PartitionState.PState_Normal)
                    .addAllShards(List.of(shard))
                    .build();
            storeInfoMeta.updateShardGroup(shardGroup);

            var partitionShard = service.getPartitionByCode("graph0", lastId);
            if (partitionShard != null){
                lastId = partitionShard.getPartition().getEndKey();
            }
        }

    }
}
