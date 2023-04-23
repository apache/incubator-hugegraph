package com.baidu.hugegraph.store.meta.asynctask;

import com.baidu.scan.safesdk.SafeObjectInputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

@Slf4j
public abstract class AbstractAsyncTask implements AsyncTask, Serializable {
    private String id;
    private int partitionId;

    private String graphName;

    private AsyncTaskState state;
    private String type;
    /**
     * 任务额外需要的参数
     */
    private Object extra;

    public AbstractAsyncTask(int partitionId, String graphName, AsyncTaskState state, Object extra) {
        this.id = getNextId();
        this.partitionId = partitionId;
        this.graphName = graphName;
        this.state = state;
        this.type = getType();
        this.extra = extra;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    @Override
    public void setState(AsyncTaskState newState) {
        this.state = newState;
    }

    public AsyncTaskState getState() {
        return this.state;
    }

    @Override
    public String getGraphName() {
        return this.graphName;
    }

    public Object getExtra() {
        return this.extra;
    }

    public abstract String getType();

    private static String getNextId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    @Override
    public byte[] toBytes() {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException e) {
            log.error("AsyncTask serialized failed, {}", e.getMessage());
            e.printStackTrace();
        }
        return bytes;
    }

    public static AsyncTask fromBytes(byte[] bytes) {
        AsyncTask obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            SafeObjectInputStream ois = new SafeObjectInputStream(bis);
            obj = (AsyncTask) ois.readObject("*");
            ois.close();
            bis.close();
        } catch (IOException e) {
            log.error("AsyncTask deserialized failed,{}", e.getMessage());
        } catch (ClassNotFoundException e) {
            log.error("AsyncTask deserialized failed,{}", e.getMessage());
        }
        return obj;
    }

    @Override
    public void handleTask() {
        if (this.getState() == AsyncTaskState.FAILED) {
            onError();
        } else if (this.getState() == AsyncTaskState.START) {
            onNotFinished();
        }
    }

    protected abstract void onError();

    protected abstract void onNotFinished();

    @Override
    public String toString() {
        return "AbstractAsyncTask{" +
                "id='" + id + '\'' +
                ", partitionId=" + partitionId +
                ", graphName='" + graphName + '\'' +
                ", state=" + state +
                ", type='" + type + '\'' +
                ", extra=" + getExtra() +
                '}';
    }
}
