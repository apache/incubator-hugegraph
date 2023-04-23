package com.baidu.hugegraph.store.meta.asynctask;

public interface AsyncTask {

    /**
     * 需要检查异步任务时候，检查当前的状态，根据状态去做对应的处理
     */
    void handleTask();

    /**
     * 任务ID
     */
    String getId();

    /**
     * 针对哪个图的
     */
    String getGraphName();

    /**
     * 针对哪个分区的
     */
    int getPartitionId();

    /**
     * 用来进行序列化
     * @return
     */
    byte[] toBytes();

    /**
     * 设置执行状态
     * @param newState
     */
    void setState(AsyncTaskState newState);
}
