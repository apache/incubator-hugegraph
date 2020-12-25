/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.task;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;

public interface TaskScheduler {

    public HugeGraph graph();

    public int pendingTasks();

    public <V> void restoreTasks();

    public <V> Future<?> schedule(HugeTask<V> task);

    public <V> void cancel(HugeTask<V> task);

    public <V> void save(HugeTask<V> task);

    public <V> HugeTask<V> delete(Id id);

    public <V> HugeTask<V> task(Id id);
    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids);
    public <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                           long limit, String page);

    public boolean close();

    public int taskInputSizeLimit();
    public int taskResultSizeLimit();

    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                  throws TimeoutException;

    public <V> HugeTask<V> waitUntilTaskCompleted(Id id)
                                                  throws TimeoutException;

    public void waitUntilAllTasksCompleted(long seconds)
                                           throws TimeoutException;

    default public void checkRequirement(String op) {
        // pass
    }
}
