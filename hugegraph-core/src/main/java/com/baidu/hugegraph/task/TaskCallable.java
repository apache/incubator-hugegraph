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

import java.util.Date;
import java.util.concurrent.Callable;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.util.E;

public abstract class TaskCallable<V> implements Callable<V> {

    private HugeTask<V> task = null;
    private HugeGraph graph = null;

    private volatile long lastSaveTime = System.currentTimeMillis();
    private volatile long saveInterval = 1000 * 30;

    public TaskCallable() {
        // pass
    }

    protected void done() {
        // Do nothing, subclasses may override this method
    }

    protected void cancelled() {
        // Do nothing, subclasses may override this method
    }

    public void setMinSaveInterval(long seconds) {
        E.checkArgument(seconds > 0,
                        "Must set interval > 0, bug got '%s'", seconds);
        this.saveInterval = seconds * 1000L;
    }

    public void updateProgress(int progress) {
        HugeTask<V> task = this.task();
        task.progress(progress);

        long elapse = System.currentTimeMillis() - this.lastSaveTime;
        if (elapse > this.saveInterval) {
            this.save();
            this.lastSaveTime = System.currentTimeMillis();
        }
    }

    protected void save() {
        HugeTask<V> task = this.task();
        task.updateTime(new Date());
        task.save();
    }

    protected void graph(HugeGraph graph) {
        this.graph = graph;
    }

    public HugeGraph graph() {
        E.checkState(this.graph != null,
                     "Can't call graph() before scheduling task");
        return this.graph;
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

    public static abstract class SysTaskCallable<V> extends TaskCallable<V> {

        private HugeGraphParams params = null;

        protected void params(HugeGraphParams params) {
            this.params = params;
        }

        protected HugeGraphParams params() {
            E.checkState(this.params != null,
                         "Can't call scheduler() before scheduling task");
            return this.params;
        }
    }
}
