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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.task.TaskManager.ContextCallable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;


public abstract class TaskScheduler {

    protected static final HugeGraphLogger LOGGER
        = Log.getLogger(TaskScheduler.class);

    protected final HugeGraphParams graph;
    protected final ServerInfoManager serverManager;
    protected EventListener eventListener = null;

    protected volatile TaskTransaction taskTx = null;

    public TaskScheduler(
            HugeGraphParams graph,
            ExecutorService serverInfoDbExecutor) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.serverManager = new ServerInfoManager(graph, serverInfoDbExecutor);
    }

    public TaskScheduler(TaskScheduler another) {
            this.graph = another.graph;
            this.serverManager = another.serverManager;
    }

    public abstract HugeGraph graph();

    public abstract int pendingTasks();

    public abstract <V> void restoreTasks();

    public abstract <V> Future<?> schedule(HugeTask<V> task);

    public abstract <V> void cancel(HugeTask<V> task);

    public abstract <V> void save(HugeTask<V> task);

    public abstract <V> HugeTask<V> delete(Id id, boolean force);

    protected abstract void taskDone(HugeTask<?> task);

    public <V> HugeTask<V> delete(Id id) {
        return this.delete(id, false);
    }

    public abstract <V> HugeTask<V> task(Id id);
    public abstract <V> Iterator<HugeTask<V>> tasks(List<Id> ids);
    public abstract <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                           long limit, String page);

    public abstract boolean close();

    public abstract <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                  throws TimeoutException;

    public abstract <V> HugeTask<V> waitUntilTaskCompleted(Id id)
                                                  throws TimeoutException;

    public abstract void waitUntilAllTasksCompleted(long seconds)
                                           throws TimeoutException;

    public abstract void checkRequirement(String op);

    protected ServerInfoManager serverManager() {
        return this.serverManager;
    }

    protected abstract <V> V call(Callable<V> callable);

    /**
     * Common call method
     * @param <V>
     * @param callable
     * @param executor
     * @return
     */
    protected <V> V call(Callable<V> callable, ExecutorService executor) {
        try {
            callable = new ContextCallable<>(callable);
            return executor.submit(callable).get();
        } catch (Exception e) {
            LOGGER.logCriticalError(e, "");
            throw new HugeException("Failed to update/query TaskStore: %s",
            e, e.toString());
        }
    }

    protected TaskTransaction tx() {
        // NOTE: only the owner thread can access task tx
        if (this.taskTx == null) {
            /*
                * NOTE: don't synchronized(this) due to scheduler thread hold
                * this lock through scheduleTasks(), then query tasks and wait
                * for db-worker thread after call(), the tx may not be initialized
                * but can't catch this lock, then cause dead lock.
                * We just use this.eventListener as a monitor here
                */
            synchronized (this.eventListener) {
                if (this.taskTx == null) {
                    BackendStore store = this.graph.loadSystemStore();
                    TaskTransaction tx = new TaskTransaction(this.graph, store);
                    assert this.taskTx == null; // may be reentrant?
                    this.taskTx = tx;
                }
            }
        }
        assert this.taskTx != null;
        return this.taskTx;
    }
}
