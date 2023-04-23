package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.MetaTask;
import com.baidu.hugegraph.store.meta.asynctask.AbstractAsyncTask;
import com.baidu.hugegraph.store.meta.asynctask.AsyncTask;
import com.baidu.hugegraph.store.meta.asynctask.AsyncTaskState;
import com.baidu.hugegraph.store.meta.base.DBSessionBuilder;
import com.baidu.hugegraph.store.meta.base.PartitionMetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TaskManager extends PartitionMetaStore {

    public TaskManager(DBSessionBuilder sessionBuilder, int partId) {
        super(sessionBuilder, partId);
    }

    public void updateTask(int partId, String type, long taskId, byte[] task) {
        byte[] key = MetadataKeyHelper.getTaskKey(partId, type, taskId);
        put(key, task);
    }

    public void updateTask(MetaTask.Task task) {
        if (task.getState().compareTo(MetaTask.TaskState.Task_Stop) < 0) {
            updateTask(task.getPartition().getId(),
                    task.getType().name(), task.getId(), task.toByteArray());
        }
        else {
            deleteTask(task);
        }
    }

    public MetaTask.Task getOneTask(int partId, String type) {
        byte[] key = MetadataKeyHelper.getTaskPrefix(partId, type);
        List<MetaTask.Task> tasks = scan(MetaTask.Task.parser(), key);
        if (tasks.size() > 0) {
            return tasks.get(tasks.size() - 1);
        }
        return get(MetaTask.Task.parser(), key);
    }

    public MetaTask.Task getOneTask(int partId, MetaTask.TaskType taskType) {
        return getOneTask(partId, taskType.name());
    }

    public MetaTask.Task getOneTask(MetaTask.Task task) {
        return getOneTask(task.getPartition().getId(), task.getType());
    }

    public void deleteTask(MetaTask.Task task) {
        byte[] key = MetadataKeyHelper.getTaskKey(task.getPartition().getId(),
                task.getType().name(), task.getId());
        delete(key);
    }

    public void deleteTask(int partId, String type) {
        deletePrefix(MetadataKeyHelper.getTaskPrefix(partId, type));
    }

    public boolean taskExists(int partId, String graphName, String taskTypeName){
        return partitionTaskRepeat(partId, graphName, taskTypeName, 0);
    }

    public boolean taskExists(MetaTask.Task task){
        return null != getOneTask(task);
    }

    /*
     * 判断相同分区下相同任务是否重复
     * partId 分区id
     * TaskTypeName 任务类型名称
     * graphName
     */
    public boolean partitionTaskRepeat(int partId, String graphName, String taskTypeName) {
           return partitionTaskRepeat(partId, graphName, taskTypeName, 1) ;
    }

    private boolean partitionTaskRepeat(int partId, String graphName, String taskTypeName, int checkCount) {
        byte[] key = MetadataKeyHelper.getTaskPrefix(partId, taskTypeName);
        List<MetaTask.Task> tasks = scan(MetaTask.Task.parser(), key);
        if (tasks.size() > 1) {
            int graphCount = 0;
            for (MetaTask.Task task : tasks) {
                if (task.getPartition().getGraphName().equals(graphName) &&
                        task.getState().getNumber() < MetaTask.TaskState.Task_Stop_VALUE) {
                    graphCount++;
                }
            }

            return graphCount > checkCount;
        }
        return false;
    }

    public void putAsyncTask(AsyncTask task){
         put(MetadataKeyHelper.getAsyncTaskKey(task.getPartitionId(),
                 task.getGraphName(), task.getId()), task.toBytes());
    }

    public AsyncTask getOneAsyncTask(int partId, String graphName, String taskId){
        var bytes = get(MetadataKeyHelper.getAsyncTaskKey(partId, graphName, taskId));
        if (bytes != null) {
            return AbstractAsyncTask.fromBytes(bytes);
        }
        return null;
    }

    public void updateAsyncTaskState(int partId, String graphName, String taskId, AsyncTaskState state){
        var task = getOneAsyncTask(partId, graphName, taskId);
        if (task != null) {
            task.setState(state);
            putAsyncTask(task);
        }
    }

    public List<AsyncTask> scanAsyncTasks(int partitionId, String graphName){
        var list = new ArrayList<AsyncTask>();
        for (var task : scan(MetadataKeyHelper.getAsyncTaskPrefix(partitionId, graphName))){
            list.add(AbstractAsyncTask.fromBytes(task.value));
        }
        return list;
    }


}
