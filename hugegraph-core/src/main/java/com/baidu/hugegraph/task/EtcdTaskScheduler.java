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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import com.alipay.remoting.util.ConcurrentHashSet;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.job.EphemeralJob;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.meta.lock.LockResult;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.TaskCallable.SysTaskCallable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * EtcdTaskScheduler handle the distributed task by etcd
 * @author Scorpiour
 * @since 2022-01-01
 */
public class EtcdTaskScheduler extends TaskScheduler {

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private static final String TASK_COUNT_LOCK = "TASK_COUNT_LOCK";

    private static final String TASK_NAME_PREFIX = "etcd-task-worker";

    private final ExecutorService producer = ExecutorUtil.newFixedThreadPool(1, EtcdTaskScheduler.class.getName() + "-task-producer");

    private final Map<TaskPriority, ExecutorService> taskExecutorMap;

    private final ExecutorService backupForLoadTaskExecutor;
    private final ExecutorService taskDBExecutor;

    // Mark if a task has been visited, filter duplicate events
    private final Set<String> visitedTasks = new ConcurrentHashSet<>();

    // Mark if a task is processed by current node
    private final Map<Id, HugeTask<?>> taskMap = new ConcurrentHashMap<>();

    // Mark if Scheduler is closing
    private volatile boolean closing = false;

    /**
     * State table to switch task status under etcd
     */
    private static final Map<TaskStatus, ImmutableSet<TaskStatus>> TASK_STATUS_MAP = 
        new ImmutableMap.Builder<TaskStatus, ImmutableSet<TaskStatus>>() 
            .put(TaskStatus.UNKNOWN,
                ImmutableSet.of())
            .put(TaskStatus.NEW,
                ImmutableSet.of(
                    TaskStatus.NEW, TaskStatus.SCHEDULING))
            .put(TaskStatus.SCHEDULING,
                ImmutableSet.of(
                    TaskStatus.SCHEDULING, TaskStatus.SCHEDULED, TaskStatus.CANCELLING, TaskStatus.FAILED))
            .put(TaskStatus.SCHEDULED,
                ImmutableSet.of(
                    TaskStatus.SCHEDULED, TaskStatus.QUEUED, TaskStatus.CANCELLING, TaskStatus.RESTORING))
            .put(TaskStatus.QUEUED,
                ImmutableSet.of(
                    TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.HANGING, TaskStatus.CANCELLING, TaskStatus.RESTORING))
            .put(TaskStatus.RUNNING,
                ImmutableSet.of(
                    TaskStatus.RUNNING, TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.RESTORING))
            .put(TaskStatus.CANCELLING,
                ImmutableSet.of(
                    TaskStatus.CANCELLING, TaskStatus.CANCELLED))
            .put(TaskStatus.CANCELLED,
                ImmutableSet.of(
                    TaskStatus.CANCELLED))
            .put(TaskStatus.SUCCESS,
                ImmutableSet.of(
                    TaskStatus.SUCCESS))
            .put(TaskStatus.FAILED,
                ImmutableSet.of(
                    TaskStatus.FAILED))
            .put(TaskStatus.HANGING,
                ImmutableSet.of(
                    TaskStatus.HANGING, TaskStatus.RESTORING
                ))
            .put(TaskStatus.RESTORING,
                ImmutableSet.of(
                    TaskStatus.RESTORING, TaskStatus.QUEUED))
            .build();
 

    /**
     * Indicates that if the task has been checked already to reduce load
     */
    private final Set<String> checkedTasks = new HashSet<>();

    public EtcdTaskScheduler(
        HugeGraphParams graph,
        ExecutorService backupForLoadTaskExecutor,
        ExecutorService taskDBExecutor,
        ExecutorService serverInfoDbExecutor,
        TaskPriority maxDepth
    ) {
        super(graph, serverInfoDbExecutor);
        this.taskExecutorMap = new ConcurrentHashMap<>();

        for (int i = 0; i <= maxDepth.getValue(); i++) {
            TaskPriority priority = TaskPriority.fromValue(i);
            int poolSize = Math.max(1, CPU_COUNT - i);
            String poolName = TASK_NAME_PREFIX + "-" + priority.name();
            taskExecutorMap.putIfAbsent(priority, ExecutorUtil.newFixedThreadPool(poolSize, poolName));
        }

        this.backupForLoadTaskExecutor = backupForLoadTaskExecutor;
        this.taskDBExecutor = taskDBExecutor;

        this.eventListener =  this.listenChanges();
        MetaManager manager = MetaManager.instance();
        for (int i = 0; i <= maxDepth.getValue(); i++) {
            TaskPriority priority = TaskPriority.fromValue(i);
            manager.listenTaskAdded(this.graphSpace(), priority, this::taskEventHandler);
        }
    }

    @Override
    public int pendingTasks() {
        int count = 0;
        MetaManager manager = MetaManager.instance();
        LockResult result = null;
        try {
            result = manager.lock(EtcdTaskScheduler.TASK_COUNT_LOCK);
                if (result.lockSuccess()) {
                for(TaskStatus status : TaskStatus.PENDING_STATUSES) {
                    count +=  manager.countTaskByStatus(this.graphSpace(), status);
                }
            }
        } catch (Throwable e) {

        } finally {
            manager.unlock(TASK_COUNT_LOCK, result);
        }
        return count;
    }

    private static <V> LockResult lockTask(String graphSpace, HugeTask<V> task) {
        if (task.lockResult() != null) {
            return task.lockResult();
        }
        return MetaManager.instance().lockTask(graphSpace, task);
    }

    private static <V> void unlockTask(String graphSpace, HugeTask<V> task) {
        if (task.lockResult() == null) {
            return;
        }
        MetaManager manager = MetaManager.instance();
        manager.unlockTask(graphSpace, task);
    }

    private <V> void restoreTaskFromStatus(TaskStatus status) {
        MetaManager manager = MetaManager.instance();
        List<HugeTask<V>> taskList = manager.listTasksByStatus(this.graphSpace(), status);

        for(HugeTask<V> task : taskList) {
            if (null == task) {
                continue;
            }
            
            LockResult result = EtcdTaskScheduler.lockTask(this.graphSpace(), task);
            if (result.lockSuccess()) {
                task.lockResult(result);
                this.visitedTasks.add(task.id().asString());
                
                TaskStatus current = manager.getTaskStatus(this.graphSpace(), task);
                if (current != status) {
                    EtcdTaskScheduler.unlockTask(this.graphSpace(), task);
                    continue;
                }
                ExecutorService executor = this.pickExecutor(task.priority());
                if (null == executor) {
                    continue;
                }

                // process cancelling first
                if (current == TaskStatus.CANCELLING) {
                    EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), task, TaskStatus.CANCELLED);
                    continue;
                }

                // pickup others
                EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), task, TaskStatus.RESTORING);
                // Attach callable info
                TaskCallable<?> callable = task.callable();
                // Attach graph info
                callable.graph(this.graph());
                if (callable instanceof SysTaskCallable) {
                    ((SysTaskCallable<?>)callable).params(this.graph);
                }
                // Update status to queued
                EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), task, TaskStatus.QUEUED);
                // Update local cache
                this.taskMap.put(task.id(), task);
                this.visitedTasks.add(task.id().asString());
                task.scheduler(this);
                // Update retry
                EtcdTaskScheduler.updateTaskRetry(this.graphSpace(), task);
                executor.submit(new TaskRunner<>(task, this.graph));
            }
        }
    }

    private <V> void restorePendingTasks() {
            this.restoreTaskFromStatus(TaskStatus.HANGING);
    }

    /**
     * headless tasks are those in status of queuing / running / cancelling but not locked.
     * When node is down, the lease of the task will be lost, that means we should pick them up
     * and schedule again
     * 
     * @param <V>
     */
    private <V> void restoreHeadlessTasks() {
            for (TaskStatus status: TaskStatus.RESTORABLE_STATUSES) {
               this.restoreTaskFromStatus(status);
            }
    }

    @Override
    public <V> void restoreTasks() {
        this.restorePendingTasks();
        this.restoreHeadlessTasks();
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        if (task.callable() instanceof EphemeralJob) {
            task.status(TaskStatus.QUEUED);
            return this.submitEphemeralTask(task);
        }
        
        return this.submitTask(task);
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        try {
            EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), task, TaskStatus.CANCELLING);
            task.cancel(true);
        } catch (Throwable e) {

        }
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
        MetaManager manager = MetaManager.instance();

        HugeTask<V> task = manager.getTask(this.graphSpace(), id);
        if (null != task) {
            manager.deleteTask(this.graphSpace(), task); 
        }
        return task;
    }

    @Override 
    public void flushAllTask() {
        MetaManager.instance().flushAllTasks(this.graphSpace());
    }

    @Override
    public <V> HugeTask<V> task(Id id) {
        /** Here we have three scenarios:
         * 1. The task is handled by current node
         * 2. The task is handled by other node, but having snapshot here
         * 3. No any info here
         * As a result, we should distinguish them and processed separately
         * for case 1, we grab the info locally directly
         * for case 2, we grab the basic info and update related info, like status & progress
         * for case 3, we load everything from etcd and cached the snapshot locally for further use 
        */
        HugeTask<V> potential = (HugeTask<V>)this.taskMap.get(id);
        if (potential == null) {
            MetaManager manager = MetaManager.instance();
            potential = manager.getTask(this.graphSpace(), id);
        } else {
            return potential;
        }

        // attach task stored info
        HugeTask<V> persisted = this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return HugeTask.fromVertex(vertex);
        });
        if (null != persisted && null != potential) {
            persisted.status(potential.status());
            persisted.priority(potential.priority());
            persisted.progress(potential.progress());
        }

        this.taskMap.put(id, persisted);

        return persisted;
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
        Set<Id> idSet = new HashSet<>(ids); 
        MetaManager manager = MetaManager.instance();
        List<String> allTasks = manager.listTasks(this.graphSpace());
        
        List<HugeTask<V>> tasks = allTasks.stream().map((jsonStr) -> {
            HugeTask<V> task = TaskSerializer.fromJson(jsonStr);
            return task;
        })
        .filter((task) -> idSet.contains(task.id()))
        .map((task) -> {
            // Attach callable info
            TaskCallable<?> callable = task.callable();
            // Attach graph info
            callable.graph(this.graph());
            if (callable instanceof SysTaskCallable) {
                ((SysTaskCallable<?>)callable).params(this.graph);
            }
            task.progress(manager.getTaskProgress(this.graphSpace(), task));
            task.status(manager.getTaskStatus(this.graphSpace(), task));
            task.retries(manager.getTaskRetry(this.graphSpace(), task));
            return task;
        })
        .collect(Collectors.toList());

        Iterator<HugeTask<V>> iterator = tasks.iterator();
        return iterator;
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(TaskStatus status, long limit, String page) {
        MetaManager manager = MetaManager.instance();
        List<HugeTask<V>> tasks = manager.listTasksByStatus(this.graphSpace(), status);

        tasks.stream().forEach((task) -> {
            TaskCallable<?> callable = task.callable();
            // Attach graph info
            callable.graph(this.graph());
            if (callable instanceof SysTaskCallable) {
                ((SysTaskCallable<?>)callable).params(this.graph);
            }
            task.scheduler(this);

            task.progress(manager.getTaskProgress(this.graphSpace(), task));
            task.retries(manager.getTaskRetry(this.graphSpace(), task));
            TaskStatus now = manager.getTaskStatus(this.graphSpace(), task);
            task.status(now);
        });
        
        return tasks.iterator();
    }

    @Override
    public boolean close() {
        this.closing = true;
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
        // Mark all tasks that has not been run into pending
        this.hangTasks();
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

    /**
     * When scheduler is going to close, the tasks that has not been 
     */
    private void hangTasks() {
        for(Map.Entry<Id, HugeTask<?>> entry : this.taskMap.entrySet()) {
            HugeTask<?> task = entry.getValue();
            if (null == task) {
                continue;
            }
            synchronized(task) {
                if (!TaskStatus.UNBREAKABLE_STATUSES.contains(task.status())) {
                    task.status(TaskStatus.HANGING);
                }
            }
        }

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

    private long incompleteTaskCount() {
        return this.taskMap.values().stream().filter(task -> !TaskStatus.COMPLETED_STATUSES.contains(task.status())).count();
    }

    @Override
    public void waitUntilAllTasksCompleted(long seconds) throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        long taskSize = 0;
        for (long pass = 0;; pass++) {
            taskSize = this.incompleteTaskCount();
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
        assert !this.taskMap.containsKey(task.id()) : task;

        ExecutorService executorService = this.pickExecutor(task.priority());
        if (null == executorService) {
            return this.backupForLoadTaskExecutor.submit(task);   
        }

        int size = this.taskMap.size();
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

        this.taskMap.put(task.id(), task);
        if (this.graph().mode().loading()) {
            return this.backupForLoadTaskExecutor.submit(task);   
        }
        return executorService.submit(task);
    }

    private <V> Future<?> submitTask(HugeTask<V> task) {
        task.scheduler(this);
        task.status(TaskStatus.SCHEDULING);

        // Save task first
        this.saveWithId(task);
        // Submit to etcd
        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());
        if (callable instanceof SysTaskCallable) {
            ((SysTaskCallable<V>)callable).params(this.graph);
        }

        return this.producer.submit(new TaskCreator<V>(task, this.graph));
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
        if (closing) {
            return;
        }
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
     * Internal Producer is use to create task info to etcd
     */
    private static class TaskCreator<V> implements Runnable {

        private final HugeTask<V> task;
        private final HugeGraphParams graph;
        private final String graphSpace;

        public TaskCreator(HugeTask<V> task, HugeGraphParams graph) {
            this.task = task;
            this.graph = graph;
            this.graphSpace = this.graph.graph().graphSpace();
        }

        @Override
        public void run() {
            Thread ct = Thread.currentThread();
            MetaManager manager = MetaManager.instance();
            try {
                LockResult result = EtcdTaskScheduler.lockTask(graphSpace, task);
                if (result.lockSuccess()) {
                    task.lockResult(result);
                    TaskStatus status = manager.getTaskStatus(this.graphSpace, task);
                    // Only unknown status indicates that the task has not been located
                    if (status != TaskStatus.UNKNOWN) {
                        return;
                    }
                    manager.createTask(graphSpace, task);
                    EtcdTaskScheduler.updateTaskStatus(graphSpace, task, TaskStatus.SCHEDULED);
                }
            } catch (Throwable e) {

            } finally {
                EtcdTaskScheduler.unlockTask(graphSpace, task);
            }
        }
    }

    /**
     * Internal Producer is use to process task
     */
    private static class TaskRunner<V> implements Runnable {

        private final HugeTask<V> task;
        private final HugeGraphParams graph;
        private final String graphSpace;

        public TaskRunner(HugeTask<V> task, HugeGraphParams graph) {
            this.task = task;
            this.graph = graph;
            this.graphSpace = this.graph.graph().graphSpace();
        }

        @Override
        public void run() {
            try {
                TaskStatus etcdStatus = MetaManager.instance().getTaskStatus(this.graph.graph().graphSpace(), task);
                if (TaskStatus.COMPLETED_STATUSES.contains(etcdStatus)) {
                    return;
                } else if(task.status() == TaskStatus.CANCELLING || etcdStatus == TaskStatus.CANCELLING) {
                    EtcdTaskScheduler.updateTaskStatus(graphSpace, task, TaskStatus.CANCELLED);
                    return;
                }

                // Detect if task is mark to hanging
                synchronized(task) {
                    if (task.status() == TaskStatus.HANGING) {
                        EtcdTaskScheduler.updateTaskStatus(graphSpace, task, TaskStatus.HANGING);
                        return;
                    }
                }

                EtcdTaskScheduler.updateTaskStatus(graphSpace, task, TaskStatus.RUNNING);
                this.task.run();
                String result = task.result();

                if (!Strings.isNullOrEmpty(result) ) {
                    task.scheduler().save(task);
                }
                EtcdTaskScheduler.updateTaskProgress(graphSpace, task, task.progress());
                EtcdTaskScheduler.updateTaskStatus(graphSpace, task, TaskStatus.SUCCESS);
                
            } catch (Exception e) {
                LOGGER.logCriticalError(e, String.format("task %d %s failed due to fatal error", task.id().asString(), task.name()));
                EtcdTaskScheduler.updateTaskStatus(graphSpace, task, TaskStatus.FAILED);
            } finally {
                MetaManager.instance().unlockTask(this.graph.graph().graphSpace(), task);
            }
        }
    }

    private static boolean isTaskNextStatus(TaskStatus prevStatus, TaskStatus nextStatus) {
        return EtcdTaskScheduler.TASK_STATUS_MAP.get(prevStatus).contains(nextStatus);
    }


    private static void updateTaskRetry(String graphSpace, HugeTask<?> task) {
        MetaManager manager = MetaManager.instance();
        task.retry();
        manager.updateTaskRetry(graphSpace, task);
    }

    /**
     * Internal TaskUpdater is used to update task status
     */
    private static void updateTaskStatus(String graphSpace, HugeTask<?> task, TaskStatus nextStatus) {
        MetaManager manager = MetaManager.instance();
        // Synchronize local status & remote status
        TaskStatus etcdStatus = manager.getTaskStatus(graphSpace, task);
        /**
         * local status different to etcd status, and delayed
         */
        if (!EtcdTaskScheduler.isTaskNextStatus(etcdStatus, task.status()) && etcdStatus != TaskStatus.UNKNOWN) {
            task.status(etcdStatus);
        }
        // Ensure that next status is available
        TaskStatus prevStatus = task.status();
        if (EtcdTaskScheduler.isTaskNextStatus(prevStatus, nextStatus)) {
            task.status(nextStatus);
            manager.migrateTaskStatus(graphSpace, task, etcdStatus);
        }
    }

    /**
     * Update task progress, make it always lead
     */
    private static void updateTaskProgress(String graphSpace, HugeTask<?> task, int nextProgress) {
        MetaManager manager = MetaManager.instance();
        int etcdProgress = manager.getTaskProgress(graphSpace, task);
        // push task current progress forward
        if (task.progress() < nextProgress) {
            task.progress(nextProgress);
        }
        if (etcdProgress > task.progress()) {
            task.progress(etcdProgress);
            return;
        }
        manager.updateTaskProgress(graphSpace, task);
    }

    private ExecutorService pickExecutor(TaskPriority priority) {
        ExecutorService result = this.taskExecutorMap.get(priority);
        if (null != result) {
            return result;
        }
        for (int i = priority.getValue() + 1; i <= TaskPriority.LOW.getValue(); i++) {
            TaskPriority nextPriority = TaskPriority.fromValue(i);
            result = this.taskExecutorMap.get(nextPriority);
            if (null != result) {
                return result;
            }
        }
        return null;
    }

    /**
     * General handler of tasks
     * @param <T>
     * @param response
     */
    private <T> void taskEventHandler(T response) {

        if (this.closing) {
            return;
        }
        
        // Prepare events
        MetaManager manager = MetaManager.instance();
        Map<String, String> events = manager.extractKVFromResponse(response);

        // Since the etcd event is not a single task, we should cope with them one by one
        for(Map.Entry<String, String> entry : events.entrySet()) {
            // If the task has been checked already, skip
            if (this.checkedTasks.contains(entry.getKey())) {
                continue;
            }
            try {
                // Deserialize task
                HugeTask<?> task = TaskSerializer.fromJson(entry.getValue());

                // Try to lock the task
                LockResult result = EtcdTaskScheduler.lockTask(this.graphSpace(), task);
                if (result.lockSuccess()) {
                    // Persist the lockResult instance to task for keepAlive and unlock
                    task.lockResult(result);
                    // The task has been visited once
                    if (this.visitedTasks.contains(task.id().asString())) {
                        EtcdTaskScheduler.unlockTask(this.graphSpace(), task);
                        continue;
                    }
                    // Mark the task is visited already
                    this.visitedTasks.add(task.id().asString());
                    // Pick executor
                    TaskPriority priority = task.priority();
                    ExecutorService executor = this.pickExecutor(priority);
                    if (null == executor) {
                        EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), task, TaskStatus.SCHEDULED);
                        return;
                    }
                    // Grab status info from task
                    TaskStatus currentStatus = manager.getTaskStatus(this.graphSpace(), task);
                    task.status(currentStatus);
                    // If task has been occupied, skip also
                    if (TaskStatus.OCCUPIED_STATUS.contains(currentStatus)) {
                        this.visitedTasks.add(task.id().asString());
                        EtcdTaskScheduler.unlockTask(this.graphSpace(), task);
                        continue;
                    }
                    // Attach callable info
                    TaskCallable<?> callable = task.callable();
                    // Attach priority info
                    MetaManager.instance().attachTaskInfo(task, entry.getKey());
                    // Attach graph info
                    callable.graph(this.graph());
                    if (callable instanceof SysTaskCallable) {
                        ((SysTaskCallable<?>)callable).params(this.graph);
                    }
                    // Update status to queued
                    this.taskMap.put(task.id(), task);
                    task.scheduler(this);
                    EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), task, TaskStatus.QUEUED);
                    // run it
                    executor.submit(new TaskRunner<>(task, this.graph));
                }
            } catch (Exception e) {
                LOGGER.logCriticalError(e, "Handle task failed");
            }
        }
    }
}