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

import java.util.concurrent.Callable;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.util.E;

public abstract class TaskCallable<V> implements Callable<V> {

    private TaskScheduler scheduler = null;
    private HugeTask<V> task = null;

    public TaskCallable() {
        // pass
    }

    protected void done() {
        // Do nothing, subclasses may override this method
    }

    protected void cancelled() {
        // Do nothing, subclasses may override this method
    }

    public HugeGraph graph() {
        return this.scheduler().graph();
    }

    protected void scheduler(TaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public TaskScheduler scheduler() {
        E.checkState(this.scheduler != null,
                     "Can't call scheduler() before scheduling task");
        return this.scheduler;
    }

    protected void task(HugeTask<V> task) {
        this.task = task;
    }

    public HugeTask<V> task() {
        E.checkState(this.task != null,
                     "Can't call task() before scheduling task");
        return this.task;
    }

    @SuppressWarnings("unchecked")
    public static <V> TaskCallable<V> fromClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (TaskCallable<V>) clazz.newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to load task: %s", e, className);
        }
    }

    public static <V> TaskCallable<V> empty(Exception e) {
        return new TaskCallable<V>() {
            @Override
            public V call() throws Exception {
                throw e;
            }
        };
    }
}
