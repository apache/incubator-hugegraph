package com.baidu.hugegraph.store.cmd;

import com.baidu.hugegraph.pd.grpc.pulse.CleanPartition;
import com.baidu.hugegraph.pd.grpc.pulse.CleanType;
import com.baidu.hugegraph.store.meta.Partition;
import lombok.Data;


@Data
public class CleanDataRequest extends HgCmdBase.BaseRequest{

    private long keyStart;
    private long keyEnd;

    private CleanType cleanType;

    private boolean deletePartition;

    private long taskId;

    @Override
    public byte magic() {
        return HgCmdBase.CLEAN_DATA;
    }

    public static CleanDataRequest fromCleanPartitionTask(CleanPartition task, Partition partition, long taskId) {
        return fromCleanPartitionTask(partition.getGraphName(), partition.getId(), taskId, task);
    }

    public static CleanDataRequest fromCleanPartitionTask(String graphName, int partitionId, long taskId,
                                                          CleanPartition task){
        CleanDataRequest request = new CleanDataRequest();
        request.setGraphName(graphName);
        request.setPartitionId(partitionId);
        request.setCleanType(task.getCleanType());
        request.setKeyStart(task.getKeyStart());
        request.setKeyEnd(task.getKeyEnd());
        request.setDeletePartition(task.getDeletePartition());
        request.setTaskId(taskId);
        return request;
    }

    public static CleanPartition toCleanPartitionTask(CleanDataRequest request){
        return  CleanPartition.newBuilder()
                .setKeyStart(request.keyStart)
                .setKeyEnd(request.keyEnd)
                .setDeletePartition(request.deletePartition)
                .setCleanType(request.cleanType)
                .build();
    }
}
