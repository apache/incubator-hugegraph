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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.alipay.remoting.util.ConcurrentHashSet;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.job.EphemeralJob;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.meta.lock.LockResult;
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

    private final Map<TaskPriority, BlockingQueue<Runnable>> taskQueueMap = new HashMap<>();

    private final ExecutorService taskExecutor;
    private final ExecutorService backupForLoadTaskExecutor;
    private final ExecutorService taskDBExecutor;

    private final ExecutorService executorService;

    private final Set<String> visitedTasks = new ConcurrentHashSet<>();

    private final Map<Id, HugeTask<?>> tasks = new HashMap<>();

    /**
     * Indicates that if the task has been checked already to reduce load
     */
    private final Set<String> checkedTasks = new HashSet<>();

    public EtcdTaskScheduler(
        HugeGraphParams graph,
        ExecutorService taskExecutor,
        ExecutorService backupForLoadTaskExecutor,
        ExecutorService taskDBExecutor,
        ExecutorService serverInfoDbExecutor,
        TaskPriority maxDepth
    ) {
        super(graph, serverInfoDbExecutor);
        this.taskExecutor = taskExecutor;
        this.backupForLoadTaskExecutor = backupForLoadTaskExecutor;
        this.taskDBExecutor = taskDBExecutor;

        this.eventListener =  this.listenChanges();

        BlockingQueue<Runnable> taskQueue = this.taskQueueMap.computeIfAbsent(maxDepth, v -> new LinkedBlockingQueue<>());

        this.executorService = new ThreadPoolExecutor(1, CPU_COUNT, 30, TimeUnit.SECONDS, taskQueue);
        MetaManager.instance().listenTaskAdded(this.graphSpace(), TaskPriority.NORMAL, this::taskEventHandler);
        MetaManager.instance().listenTaskAdded(this.graphSpace(), TaskPriority.NORMAL, this::extraTaskEventHandler);
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
            return this.submitEphemeralTask(task);
        }

        System.out.println("====> Going to submit task + " + task.id().asString());
        
        return this.submitTask(task);
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        // TODO Auto-generated method stub
        
    }

    private <V> Id saveWithId(HugeTask<V> task) {
        task.scheduler(this);
        E.checkArgumentNotNull(task, "Task can't be null");
        HugeVertex v = this.call(() -> {
            // Construct vertex from task
            HugeVertex vertex = this.tx().constructVertex(task);
            // Delete index of old vertex to avoid stale index
            this.tx().deleteIndex(vertex);
            // Add or update task info to backend store
            return this.tx().addVertex(vertex);
        });
        return v.id();
    }

    @Override
    public <V> void save(HugeTask<V> task) {
        this.saveWithId(task);
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

    private <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds,
                                                   long intervalMs)
                                                   throws TimeoutException {
        long passes = seconds * 1000 / intervalMs;
        HugeTask<V> task = null;
        for (long pass = 0;; pass++) {
            try {
                task = this.task(id);
            } catch (NotFoundException e) {
                if (task != null && task.completed()) {
                    assert task.id().asLong() < 0L : task.id();
                    sleep(intervalMs);
                    return task;
                }
                throw e;
            }
            if (task.completed()) {
                // Wait for task result being set after status is completed
                sleep(intervalMs);
                return task;
            }
            if (pass >= passes) {
                break;
            }
            sleep(intervalMs);
        }
        throw new TimeoutException(String.format(
                  "Task '%s' was not completed in %s seconds", id, seconds));
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds) throws TimeoutException {
        return this.waitUntilTaskCompleted(id, seconds, QUERY_INTERVAL);
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id) throws TimeoutException {
        long timeout = this.graph.configuration()
                                 .get(CoreOptions.TASK_WAIT_TIMEOUT);
        return this.waitUntilTaskCompleted(id, timeout, 1L);
    }

    @Override
    public void waitUntilAllTasksCompleted(long seconds) throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        int taskSize = 0;
        for (long pass = 0;; pass++) {
            taskSize = this.pendingTasks();
            if (taskSize == 0) {
                sleep(QUERY_INTERVAL);
                return;
            }
            if (pass >= passes) {
                break;
            }
            sleep(QUERY_INTERVAL);
        }
        throw new TimeoutException(String.format(
                  "There are still %s incomplete tasks after %s seconds",
                  taskSize, seconds));
        
    }

    private <V> Future<?> submitEphemeralTask(HugeTask<V> task) {
        assert !this.tasks.containsKey(task.id()) : task;
        int size = this.tasks.size();
        E.checkArgument(size < MAX_PENDING_TASKS,
            "Pending tasks size %s has exceeded the max limit %s",
            size + 1, MAX_PENDING_TASKS);
        task.scheduler(this);
        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());
        if (callable instanceof SysTaskCallable) {
            ((SysTaskCallable<V>)callable).params(this.graph);
        }

        this.tasks.put(task.id(), task);
        if (this.graph().mode().loading()) {
            LOGGER.logCustomDebug("Schedule task {} to backup for load task executor", "Scorpiour", task);
            return this.backupForLoadTaskExecutor.submit(task);   
        }
        return this.taskExecutor.submit(task);
    }

    private <V> Future<?> submitTask(HugeTask<V> task) {
        task.scheduler(this);

        // Save task first
        Id id = this.saveWithId(task);
        // Submit to etcd
        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());
        if (callable instanceof SysTaskCallable) {
            ((SysTaskCallable<V>)callable).params(this.graph);
        }

        System.out.println(String.format("====> Thread %d start to create task %d", Thread.currentThread().getId(), task.id().asLong()));
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

    private static boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ignored) {
            // Ignore InterruptedException
            return false;
        }
    }

    /**
     * Internal Producer is use to submit task info to etcd
     */
    private static class Producer<V> implements Runnable {

        private final HugeTask<V> task;
        private final HugeGraphParams graph;

        public Producer(HugeTask<V> task, HugeGraphParams graph) {
            this.task = task;
            this.graph = graph;
        }

        @Override
        public void run() {
            LOGGER.logCustomDebug("====> Producer runner start to write {}", "Scorpiour", task);

            System.out.println("====> Producer runner thread: " + Thread.currentThread().getId());

            MetaManager metaManager = MetaManager.instance();
            task.status(TaskStatus.SCHEDULING);
            metaManager.createTask(this.graph.graph().graphSpace(), task);

            task.status(TaskStatus.SCHEDULED);
            MetaManager.instance().updateTaskStatus(this.graph.graph().graphSpace(), task);
        }
    }

    /**
     * Internal Producer is use to submit task info to etcd
     */
    private static class Consumer<V> implements Runnable {

        private final HugeTask<V> task;
        private final HugeGraphParams graph;

        public Consumer(HugeTask<V> task, HugeGraphParams graph) {
            this.task = task;
            this.graph = graph;
        }

        @Override
        public void run() {
            LOGGER.logCustomDebug("====> Consumer runner start to write {}", "Scorpiour", task);

            System.out.println("====> consumer runner thread: " + Thread.currentThread().getId());

            TaskStatus status = MetaManager.instance().getTaskStatus(this.graph.graph().graphSpace(), task);
            if (TaskStatus.COMPLETED_STATUSES.contains(status)) {
                System.out.println("====> task is complete! consumer runner finished: " + Thread.currentThread().getId());
                return;
            }

            task.status(TaskStatus.RUNNING);
            MetaManager.instance().updateTaskStatus(this.graph.graph().graphSpace(), task);

            this.task.run();

            task.status(TaskStatus.SUCCESS);
            MetaManager.instance().updateTaskStatus(this.graph.graph().graphSpace(), task);
            MetaManager.instance().unlockTask(this.graph.graph().graphSpace(), task);

            System.out.println("====> consumer runner finished: " + Thread.currentThread().getId());
        }
    }

    /**
     * General handler of tasks
     * @param <T>
     * @param response
     */
    private <T> void taskEventHandler(T response) {

            System.out.println("====> Normal handler Current thread: " + Thread.currentThread().getId() + " name " + Thread.currentThread().getName());

            MetaManager manager = MetaManager.instance();
            Map<String, String> events = manager.extractKVFromResponse(response);

            for(Map.Entry<String, String> entry : events.entrySet()) {
                System.out.println(String.format("====> [Thread %d] task info %s, %s", Thread.currentThread().getId(), entry.getKey(), entry.getValue()));
                if (this.checkedTasks.contains(entry.getKey())) {
                    System.out.println(String.format("====> [Thread %d] task info %s has been executed", Thread.currentThread().getId(), entry.getKey()));
                    continue;
                }
                try {
                    HugeTask<?> task = TaskSerializer.fromJson(entry.getValue());

                    System.out.println(String.format("====> [Thread %d] try to lock %s", Thread.currentThread().getId(), entry.getKey()));

                    LockResult result = manager.lockTask(this.graphSpace(), task);
                    if (result.lockSuccess()) {
                        task.lockResult(result);
                        // first check if lock processed
                        if (this.visitedTasks.contains(task.id().asString())) {
                            System.out.println(String.format("====> [Thread %d] found task %s has been visited", Thread.currentThread().getId(), entry.getKey()));
                            manager.unlockTask(this.graphSpace(), task);
                            continue;
                        }
                        this.visitedTasks.add(task.id().asString());
                        TaskStatus currentStatus = manager.getTaskStatus(this.graphSpace(), task);
                        if (TaskStatus.COMPLETED_STATUSES.contains(currentStatus)) {
                            System.out.println(String.format("====> [Thread %d] found task %s has been done", Thread.currentThread().getId(), entry.getKey()));
                            manager.unlockTask(this.graphSpace(), task);
                            continue;
                        }
                        System.out.println(String.format("=====> Grab task %s lock success", task.id().asString()));
                        // attach callable info
                        TaskCallable<?> callable = task.callable();
                        // attach priority info
                        MetaManager.instance().attachTaskInfo(task, entry.getKey());
                        // attach graph info
                        callable.graph(this.graph());
                        // run it
                        task.status(TaskStatus.QUEUED);
                        MetaManager.instance().updateTaskStatus(this.graph.graph().graphSpace(), task);


                        this.taskExecutor.submit(new Consumer(task, this.graph));
                    } else {
                        System.out.println("=====> Grab task lock failed");
                    }
                } catch (Exception e) {
                    System.out.println("=====> Grab task lock error");
                    System.out.println(e);
                    System.out.println(e.getStackTrace());
                }
            }

        System.out.println(String.format("====> [Thread %d] handle response end", Thread.currentThread().getId()));
    }

    /**
     * 
     * Race handler of tasks
     * TODO: should be removed after integration
     * @param <T>
     * @param response
     */
    private <T> void extraTaskEventHandler(T response) {
            System.out.println("====> Race handler Current thread: " + Thread.currentThread().getId() + " name " + Thread.currentThread().getName());

            MetaManager manager = MetaManager.instance();
            Map<String, String> events = manager.extractKVFromResponse(response);

            for(Map.Entry<String, String> entry : events.entrySet()) {
                System.out.println(String.format("====> [Thread %d] task info %s, %s", Thread.currentThread().getId(), entry.getKey(), entry.getValue()));
                if (this.checkedTasks.contains(entry.getKey())) {
                    System.out.println(String.format("====> [Thread %d] task info %s has been executed", Thread.currentThread().getId(), entry.getKey()));
                    continue;
                }
                try {
                    HugeTask<?> task = TaskSerializer.fromJson(entry.getValue());

                    System.out.println(String.format("====> [Thread %d] try to lock %s", Thread.currentThread().getId(), entry.getKey()));

                    LockResult result = manager.lockTask(this.graphSpace(), task);
                    if (result.lockSuccess()) {
                        task.lockResult(result);
                        if (this.visitedTasks.contains(task.id().asString())) {
                            System.out.println(String.format("====> [Thread %d] found task %s has been visited", Thread.currentThread().getId(), entry.getKey()));
                            manager.unlockTask(this.graphSpace(), task);
                            continue;
                        }
                        this.visitedTasks.add(task.id().asString());
                        TaskStatus currentStatus = manager.getTaskStatus(this.graphSpace(), task);
                        if (TaskStatus.COMPLETED_STATUSES.contains(currentStatus)) {
                            System.out.println(String.format("====> [Thread %d] found task %s has been done", Thread.currentThread().getId(), entry.getKey()));
                            manager.unlockTask(this.graphSpace(), task);
                            continue;
                        }
    
                        System.out.println("=====> Grab task lock success");
                        // attach callable info
                        TaskCallable<?> callable = task.callable();
                        // attach priority info
                        MetaManager.instance().attachTaskInfo(task, entry.getKey());
                        // attach graph info
                        callable.graph(this.graph());
                        // run it
                        this.taskExecutor.submit(new Consumer(task, this.graph));
                    } else {
                        System.out.println("=====> Grab task lock failed");
                    }
                } catch (Exception e) {
                    System.out.println("=====> Grab task lock error");
                    System.out.println(e);
                    System.out.println(e.getStackTrace());
                }

            System.out.println(String.format("====> [Thread %d] handle response end", Thread.currentThread().getId()));
        }
    }
    
}