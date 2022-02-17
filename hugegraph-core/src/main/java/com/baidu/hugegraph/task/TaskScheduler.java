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

/**
 * Base class of task scheduler
 */
public abstract class TaskScheduler {

    protected static final HugeGraphLogger LOGGER
        = Log.getLogger(TaskScheduler.class);

    /**
     * Which graph the scheduler belongs to
     */
    protected final HugeGraphParams graph;
    protected final String graphName;

    /**
     * serverInfo
     */
    protected final ServerInfoManager serverManager;
    protected EventListener eventListener = null;

    /**
     * Task transactions, for persistence
     */
    protected volatile TaskTransaction taskTx = null;

    protected static final long QUERY_INTERVAL = 100L;
    protected static final int MAX_PENDING_TASKS = 10000;

    public TaskScheduler(
            HugeGraphParams graph,
            ExecutorService serverInfoDbExecutor) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.graphName = graph.name();
        this.serverManager = new ServerInfoManager(graph, serverInfoDbExecutor);
    }

    public TaskScheduler(TaskScheduler another) {
            this.graph = another.graph;
            this.graphName = another.graphName;
            this.serverManager = another.serverManager;
    }

    /**
     * Get all task that are in pending status, includes
     * queued
     * @return
     */
    public abstract int pendingTasks();

    /**
     * Restore unprocessed tasks
     * @param <V>
     */
    public abstract <V> void restoreTasks();

    /**
     * Schedule a task.
     * The Scheduled task maybe run by other physics nodes
     * @param <V>
     * @param task
     * @return
     */
    public abstract <V> Future<?> schedule(HugeTask<V> task);

    /**
     * Cancel a task if it has not been run
     * @param <V>
     * @param task
     */
    public abstract <V> void cancel(HugeTask<V> task);

    /**
     * Persist the task info
     * @param <V>
     * @param task
     */
    public abstract <V> void save(HugeTask<V> task);

    /**
     * Delete a task
     * @param <V>
     * @param id
     * @param force
     * @return
     */
    public abstract <V> HugeTask<V> delete(Id id, boolean force);

    protected abstract void taskDone(HugeTask<?> task);

    /**
     * Flush all tasks, only implemented under distribution scenario
     */
    public abstract void flushAllTask();

    public <V> HugeTask<V> delete(Id id) {
        return this.delete(id, false);
    }

    /**
     * Get info of certain task
     * @param <V>
     * @param id
     * @return
     */
    public abstract <V> HugeTask<V> task(Id id);

    /**
     * Get info of tasks
     * @param <V>
     * @param ids
     * @return
     */
    public abstract <V> Iterator<HugeTask<V>> tasks(List<Id> ids);

    /**
     * Get tasks by status
     * @param <V>
     * @param status
     * @param limit
     * @param page
     * @return
     */
    public abstract <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                           long limit, String page);

    /**
     * Scheduler is closed, will not accept more tasks
     * @return
     */
    public abstract boolean close();

    public abstract <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                  throws TimeoutException;

    public abstract <V> HugeTask<V> waitUntilTaskCompleted(Id id)
                                                  throws TimeoutException;

    public abstract void waitUntilAllTasksCompleted(long seconds)
                                           throws TimeoutException;
    public HugeGraph graph() {
        return this.graph.graph();
    }

    public String graphSpace() {
        return this.graph.graph().graphSpace();
    }

    public void checkRequirement(String op) {
        if (!this.serverManager().master()) {
            throw new HugeException("Can't %s task on non-master server", op);
        }
    }

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
