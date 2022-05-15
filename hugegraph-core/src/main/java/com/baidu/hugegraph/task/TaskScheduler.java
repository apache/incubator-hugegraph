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

    HugeGraph graph();

    int pendingTasks();

    <V> void restoreTasks();

    <V> Future<?> schedule(HugeTask<V> task);

    <V> void cancel(HugeTask<V> task);

    <V> void save(HugeTask<V> task);

    <V> HugeTask<V> delete(Id id);

    <V> HugeTask<V> task(Id id);

    <V> Iterator<HugeTask<V>> tasks(List<Id> ids);

    <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                    long limit, String page);

    boolean close();

    <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                           throws TimeoutException;

    <V> HugeTask<V> waitUntilTaskCompleted(Id id)
                                           throws TimeoutException;

    void waitUntilAllTasksCompleted(long seconds)
                                    throws TimeoutException;

    void checkRequirement(String op);
}
