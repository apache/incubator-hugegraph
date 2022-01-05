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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.job.EphemeralJob;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask.P;
import com.baidu.hugegraph.task.TaskCallable.SysTaskCallable;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.ExecutorUtil;

import com.google.common.collect.ImmutableSet;

public class EtcdTaskScheduler extends TaskScheduler {

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private final ExecutorService producer = ExecutorUtil.newFixedThreadPool(1, EtcdTaskScheduler.class.getName());

    private final Map<TaskPriority, BlockingQueue<HugeTask<?>>> taskQueueMap = new HashMap<>();

    private final ExecutorService taskDBExecutor;

    public EtcdTaskScheduler(
        HugeGraphParams graph,
        // ExecutorService taskExecutor,
        // ExecutorService backupForLoadTaskExecutor,
        ExecutorService taskDBExecutor,
        ExecutorService serverInfoDbExecutor,
        TaskPriority maxDepth
    ) {
        super(graph, serverInfoDbExecutor);
        this.taskDBExecutor = taskDBExecutor;

        this.eventListener =  this.listenChanges();
        
    }

    @Override
    public HugeGraph graph() {
        return this.graph.graph();
    }

    @Override
    public int pendingTasks() {
        return this
            .taskQueueMap
            .values()
            .stream()
            .collect(
                Collectors.summingInt(BlockingQueue::size)
            );
    }

    @Override
    public <V> void restoreTasks() {

        LOGGER.logCustomDebug("restore tasks {}", "Scorpiour", this);
        
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        if (task.callable() instanceof EphemeralJob) {
            task.status(TaskStatus.QUEUED);
            return this.submitTask(task);
        }
        
        LOGGER.logCustomDebug("restore tasks {}", "Scorpiour", task);
        return this.submitTask(task);
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public <V> void save(HugeTask<V> task) {
        task.scheduler(this);
        E.checkArgumentNotNull(task, "Task can't be null");
        this.call(() -> {
            // Construct vertex from task
            HugeVertex vertex = this.tx().constructVertex(task);
            // Delete index of old vertex to avoid stale index
            this.tx().deleteIndex(vertex);
            // Add or update task info to backend store
            return this.tx().addVertex(vertex);  
        });
        // TODO Auto-generated method stub
    }

    @Override
    public <V> HugeTask<V> delete(Id id, boolean force) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <V> HugeTask<V> task(Id id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(TaskStatus status, long limit, String page) {
        return this.call(() -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            if (null != page) {
                query.page(page);
            }
            VertexLabel label = this.graph().vertexLabel(P.TASK);
            query.eq(HugeKeys.LABEL, label.id());
            query.showHidden(true);
            if (limit >= 0) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryVertices(query);
            Iterator<HugeTask<V>> tasks =
                new MapperIterator<>(vertices, HugeTask::fromVertex);

            return QueryResults.toList(tasks);
        });
    }

    @Override
    public boolean close() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
        if (!this.taskDBExecutor.isShutdown()) {
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
            });
        }
        return this.serverManager.close();
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds) throws TimeoutException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id) throws TimeoutException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void waitUntilAllTasksCompleted(long seconds) throws TimeoutException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void checkRequirement(String op) {
        // TODO Auto-generated method stub
        
    }

    private <V> Future<?> submitTask(HugeTask<V> task) {
        task.scheduler(this);

        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());
        if (callable instanceof SysTaskCallable) {
            ((SysTaskCallable<V>)callable).params(this.graph);
        }

        if (this.graph.mode().loading()) {
            
        }
        return this.producer.submit(new Producer<V>(task, this.graph));
    }

    @Override
    protected ServerInfoManager serverManager() {
        return this.serverManager;
    }

    private <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    
    @Override
    protected <V> V call(Callable<V> callable) {
        return super.call(callable, this.taskDBExecutor);
    }

    @Override
    protected void taskDone(HugeTask<?> task) {
        try {
            this.serverManager.decreaseLoad(task.load());
        } catch (Exception e) {
            LOGGER.logCriticalError(e, "Failed to decrease load for task '{}' on server '{}'");
        }
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure task schema create after system info initialized
            if (storeEvents.contains(event.name())) {
                this.call(() -> this.tx().initSchema());
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }


    private static class Producer<V> implements Runnable {

        private final HugeTask<V> task;
        private final HugeGraphParams graph;

        public Producer(HugeTask<V> task, HugeGraphParams graph) {
            this.task = task;
            this.graph = graph;
        }

        @Override
        public void run() {
            LOGGER.logCustomDebug("Producer runner start to write {}", "Scorpiour", task);

            MetaManager metaManager = MetaManager.instance();
            metaManager.createTask(graph.name(), task);
        }
        
    }

    private static class Consumer implements Runnable {

        @Override
        public void run() {
            // TODO Auto-generated method stub
            
        }

    }
    
}