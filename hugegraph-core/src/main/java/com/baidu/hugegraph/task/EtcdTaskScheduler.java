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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.job.EphemeralJob;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.task.TaskCallable.SysTaskCallable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public class EtcdTaskScheduler extends TaskScheduler {

    private static final HugeGraphLogger LOGGER
        = Log.getLogger(TaskScheduler.class);

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private final ExecutorService producer = ExecutorUtil.newFixedThreadPool(1, EtcdTaskScheduler.class.getName());

    private final Map<TaskPriority, BlockingQueue<HugeTask<?>>> taskQueueMap = new HashMap<>();

    public EtcdTaskScheduler(
        HugeGraphParams graph,
        ExecutorService serverInfoDbExecutor,
        TaskPriority maxDepth
    ) {
        super(graph, serverInfoDbExecutor);
        
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
        return null;
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public <V> void save(HugeTask<V> task) {
        task.scheduler(this);
        BlockingQueue<HugeTask<?>> queue = this.taskQueueMap.computeIfAbsent(task.priority(), v -> new LinkedBlockingQueue<>());
        queue.add(task);
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean close() {
        // TODO Auto-generated method stub
        return false;
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
        return this.producer.submit(new Producer<V>(task));
    }

    @Override
    protected ServerInfoManager serverManager() {
        return this.serverManager;
    }

    @Override
    protected <V> V call(Callable<V> callable) {
        // TODO Auto-generated method stub
        return null;
    }

    private static class Producer<V> implements Runnable {

        private final HugeTask<V> task;

        public Producer(HugeTask<V> task) {
            this.task = task;
        }

        @Override
        public void run() {
            LOGGER.logCustomDebug("Producer runner start to write {}", "Scorpiour", task);

            MetaManager metaManager = MetaManager.instance();
            metaManager.createTask(task);
        }
        
    }

    private static class Consumer implements Runnable {

        @Override
        public void run() {
            // TODO Auto-generated method stub
            
        }

    }
    
}