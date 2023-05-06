package org.apache.hugegraph.pd.meta;

import com.baidu.hugegraph.pd.common.PDException;

import org.apache.hugegraph.pd.config.PDConfig;

import com.baidu.hugegraph.pd.grpc.MetaTask;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.pulse.MovePartition;
import com.baidu.hugegraph.pd.grpc.pulse.SplitPartition;

import java.util.List;

/**
 * 任务管理
 */
public class TaskInfoMeta extends MetadataRocksDBStore{
    public TaskInfoMeta(PDConfig pdConfig) {
        super(pdConfig);
    }

    /**
     * 添加分区分裂任务
     */
    public void addSplitTask(int groupID, Metapb.Partition partition, SplitPartition splitPartition)
            throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskKey(partition.getGraphName(), groupID);
        MetaTask.Task task = MetaTask.Task.newBuilder()
                .setType(MetaTask.TaskType.Split_Partition)
                .setState(MetaTask.TaskState.Task_Doing)
                .setStartTimestamp(System.currentTimeMillis())
                .setPartition(partition)
                .setSplitPartition(splitPartition)
                .build();
        put(key, task.toByteString().toByteArray());
    }

    public void updateSplitTask(MetaTask.Task task) throws PDException {
        var partition = task.getPartition();
        byte[] key = MetadataKeyHelper.getSplitTaskKey(partition.getGraphName(), partition.getId());
        put(key, task.toByteString().toByteArray());
    }

    public MetaTask.Task getSplitTask(String graphName, int groupID) throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskKey(graphName, groupID);
        return getOne(MetaTask.Task.parser(), key);
    }

    public List<MetaTask.Task> scanSplitTask(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getSplitTaskPrefix(graphName);
        return scanPrefix(MetaTask.Task.parser(), prefix);
    }

    public void removeSplitTaskPrefix(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskPrefix(graphName);
        removeByPrefix(key);
    }

    public boolean hasSplitTaskDoing() throws PDException {
        byte[] key = MetadataKeyHelper.getAllSplitTaskPrefix();
        return scanPrefix(key).size() > 0;
    }

    public void addMovePartitionTask(Metapb.Partition partition, MovePartition movePartition)
            throws PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskKey(partition.getGraphName(),
                movePartition.getTargetPartition().getId(), partition.getId());

        MetaTask.Task task = MetaTask.Task.newBuilder()
                .setType(MetaTask.TaskType.Move_Partition)
                .setState(MetaTask.TaskState.Task_Doing)
                .setStartTimestamp(System.currentTimeMillis())
                .setPartition(partition)
                .setMovePartition(movePartition)
                .build();
        put(key, task.toByteArray());
    }

    public void updateMovePartitionTask(MetaTask.Task task)
            throws PDException {

        byte[] key = MetadataKeyHelper.getMoveTaskKey(task.getPartition().getGraphName(),
                task.getMovePartition().getTargetPartition().getId(),
                task.getPartition().getId());
        put(key, task.toByteArray());
    }

    public MetaTask.Task getMovePartitionTask(String graphName, int targetId, int partId) throws PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskKey(graphName, targetId, partId);
        return getOne(MetaTask.Task.parser(), key);
    }

    public List<MetaTask.Task> scanMoveTask(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getMoveTaskPrefix(graphName);
        return scanPrefix(MetaTask.Task.parser(), prefix);
    }

    /**
     * 按照prefix删除迁移任务，一次分组的
     * @param graphName 图名称
     * @throws PDException io error
     */
    public void removeMoveTaskPrefix(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskPrefix(graphName);
        removeByPrefix(key);
    }

    public boolean hasMoveTaskDoing() throws PDException {
        byte[] key = MetadataKeyHelper.getAllMoveTaskPrefix();
        return scanPrefix(key).size() > 0;
    }

}
