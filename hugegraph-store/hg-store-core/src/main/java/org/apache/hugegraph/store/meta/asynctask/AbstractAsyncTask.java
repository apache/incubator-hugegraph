/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.meta.asynctask;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractAsyncTask implements AsyncTask, Serializable {

    private final String id;
    private final int partitionId;

    private final String graphName;
    private final String type;
    /**
     * Parameters needed for the task additionally
     */
    private final Object extra;
    private AsyncTaskState state;

    public AbstractAsyncTask(int partitionId, String graphName, AsyncTaskState state,
                             Object extra) {
        this.id = getNextId();
        this.partitionId = partitionId;
        this.graphName = graphName;
        this.state = state;
        this.type = getType();
        this.extra = extra;
    }

    private static String getNextId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static AsyncTask fromBytes(byte[] bytes) {
        AsyncTask obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = (AsyncTask) ois.readObject();
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
    public String getId() {
        return this.id;
    }

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    public AsyncTaskState getState() {
        return this.state;
    }

    @Override
    public void setState(AsyncTaskState newState) {
        this.state = newState;
    }

    @Override
    public String getGraphName() {
        return this.graphName;
    }

    public Object getExtra() {
        return this.extra;
    }

    public abstract String getType();

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
