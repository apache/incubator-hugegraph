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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.task.HugeTask.P;
import com.baidu.hugegraph.task.TaskManager.ContextCallable;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.util.Strings;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Base class of task scheduler
 */
public abstract class TaskScheduler {

    protected static final HugeGraphLogger LOGGER
        = Log.getLogger(TaskScheduler.class);

    protected static final long NO_LIMIT = -1L;
    protected static final long PAGE_SIZE = 500L;

    /**
     * Which graph the scheduler belongs to
     */
    protected final HugeGraphParams graph;
    protected final String graphSpace;
    protected final String graphName;

    /**
     * serverInfo
     */
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
        this.graphSpace = graph.graph().graphSpace();
        this.graphName = graph.name();
    }

    public TaskScheduler(TaskScheduler another) {
            this.graph = another.graph;
            this.graphSpace = graph.graph().graphSpace();
            this.graphName = another.graphName;
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

    protected abstract <V> V call(Callable<V> callable);
    
    protected abstract <V> V call(Runnable runnable);

    /**
     * Common call method
     * @param <V>
     * @param callable
     * @param executor
     * @return
     */
    protected <V> V call(Callable<V> callable, ExecutorService executor) {
        String prevContext = TaskManager.getContext();
        if (Strings.isBlank(prevContext)) {
            TaskManager.useFakeContext();
        }
        try {
            callable = new ContextCallable<>(callable);
            return executor.submit(callable).get();
        } catch (Exception e) {
            LOGGER.logCriticalError(e, "");
            throw new HugeException("Failed to update/query TaskStore: %s",
            e, e.toString());
        } finally {
            TaskManager.setContext(prevContext);
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

    
    protected <V> Iterator<HugeTask<V>> queryTask(String key, Object value,
                                                long limit, String page) {
        return this.queryTask(ImmutableMap.of(key, value), limit, page);
    }

    protected <V> Iterator<HugeTask<V>> queryTask(Map<String, Object> conditions,
                                                long limit, String page) {
        return this.call(() -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            if (page != null) {
                query.page(page);
            }
            VertexLabel vl = this.graph().vertexLabel(P.TASK);
            query.eq(HugeKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = this.graph().propertyKey(entry.getKey());
                query.query(Condition.eq(pk.id(), entry.getValue()));
            }
            query.showHidden(true);
            if (limit != NO_LIMIT) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryVertices(query);
            Iterator<HugeTask<V>> tasks =
                    new MapperIterator<>(vertices, HugeTask::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(tasks);
        });
    }

    protected <V> Iterator<HugeTask<V>> queryTask(List<Id> ids) {
        return this.call(() -> {
            Object[] idArray = ids.toArray(new Id[ids.size()]);
            Iterator<Vertex> vertices = this.tx().queryVertices(idArray);
            Iterator<HugeTask<V>> tasks =
                    new MapperIterator<>(vertices, HugeTask::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(tasks);
        });
    }
}
