package com.baidu.hugegraph.store.meta.asynctask;

import com.baidu.hugegraph.pd.grpc.pulse.CleanType;
import com.baidu.hugegraph.store.HgStoreEngine;
import com.baidu.hugegraph.store.cmd.CleanDataRequest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanTask extends AbstractAsyncTask{

    public CleanTask(int partitionId, String graphName, AsyncTaskState state, Object attach) {
        super(partitionId, graphName, state, attach);
    }

    @Override
    public String getType() {
        return "CLEAN_TYPE";
    }

    @Override
    protected void onError() {
        cleanTask();
    }

    @Override
    protected void onNotFinished() {
        cleanTask();
    }

    private void cleanTask(){
        log.info("CleanTask begin to run:{}", this);
        var storeEngine = HgStoreEngine.getInstance();
        if (storeEngine != null) {
            if (getExtra() != null) {
                CleanDataRequest request = (CleanDataRequest) getExtra();
                var partition = storeEngine.getPartitionManager().getPartition(getGraphName(), getPartitionId());
                // 只允许清理本分区之外的数据。 缩容等任务会造成干扰, 而且不能删除分区
                if (request.getKeyEnd() == partition.getStartKey() && request.getKeyEnd() == partition.getEndKey() &&
                    request.getCleanType() == CleanType.CLEAN_TYPE_EXCLUDE_RANGE && ! request.isDeletePartition()) {
                    storeEngine.getBusinessHandler().cleanPartition(getGraphName(), getPartitionId(),
                            request.getKeyStart(), request.getKeyEnd(), request.getCleanType());
                }
            }else {
                storeEngine.getBusinessHandler().cleanPartition(getGraphName(), getPartitionId());
            }

            storeEngine.getPartitionEngine(getPartitionId()).getTaskManager()
                    .updateAsyncTaskState(getPartitionId(), getGraphName(), getId(), AsyncTaskState.SUCCESS);
        }
    }
}
