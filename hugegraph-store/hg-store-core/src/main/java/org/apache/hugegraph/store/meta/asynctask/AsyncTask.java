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
     *
     * @return
     */
    byte[] toBytes();

    /**
     * 设置执行状态
     *
     * @param newState
     */
    void setState(AsyncTaskState newState);
}
