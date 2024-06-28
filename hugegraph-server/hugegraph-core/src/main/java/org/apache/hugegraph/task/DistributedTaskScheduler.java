/*
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

package org.apache.hugegraph.task;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

public class DistributedTaskScheduler extends TaskAndResultScheduler {
    private final long schedulePeriod;
    private static final Logger LOG = Log.logger(DistributedTaskScheduler.class);
    private final ExecutorService taskDbExecutor;
    private final ExecutorService schemaTaskExecutor;
    private final ExecutorService olapTaskExecutor;
    private final ExecutorService ephemeralTaskExecutor;
    private final ExecutorService gremlinTaskExecutor;
    private final ScheduledThreadPoolExecutor schedulerExecutor;
    private final ScheduledFuture<?> cronFuture;

    /**
     * the status of scheduler
     */
    private final AtomicBoolean closed = new AtomicBoolean(true);

    private final ConcurrentHashMap<Id, HugeTask<?>> runningTasks = new ConcurrentHashMap<>();

    public DistributedTaskScheduler(HugeGraphParams graph,
                                    ScheduledThreadPoolExecutor schedulerExecutor,
                                    ExecutorService taskDbExecutor,
                                    ExecutorService schemaTaskExecutor,
                                    ExecutorService olapTaskExecutor,
                                    ExecutorService gremlinTaskExecutor,
                                    ExecutorService ephemeralTaskExecutor,
                                    ExecutorService serverInfoDbExecutor) {
        super(graph, serverInfoDbExecutor);

        this.taskDbExecutor = taskDbExecutor;
        this.schemaTaskExecutor = schemaTaskExecutor;
        this.olapTaskExecutor = olapTaskExecutor;
        this.gremlinTaskExecutor = gremlinTaskExecutor;
        this.ephemeralTaskExecutor = ephemeralTaskExecutor;

        this.schedulerExecutor = schedulerExecutor;

        this.closed.set(false);

        this.schedulePeriod = this.graph.configuration()
                                        .get(CoreOptions.TASK_SCHEDULE_PERIOD);

        this.cronFuture = this.schedulerExecutor.scheduleWithFixedDelay(
            () -> {
                // TODO: uncomment later - graph space
                // LockUtil.lock(this.graph().spaceGraphName(), LockUtil.GRAPH_LOCK);
                LockUtil.lock("", LockUtil.GRAPH_LOCK);
                try {
                    // TODO: 使用超级管理员权限，查询任务
                    // TaskManager.useAdmin();
                    this.cronSchedule();
                } catch (Throwable t) {
                    // TODO: log with graph space
                    LOG.info("cronScheduler exception graph: {}", this.graphName(), t);
                } finally {
                    // TODO: uncomment later - graph space
                    LockUtil.unlock("", LockUtil.GRAPH_LOCK);
                    // LockUtil.unlock(this.graph().spaceGraphName(), LockUtil.GRAPH_LOCK);
                }
            },
            10L, schedulePeriod,
            TimeUnit.SECONDS);
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

    public void cronSchedule() {
        // 执行周期调度任务

        if (!this.graph.started() || this.graph.closed()) {
            return;
        }

        // 处理 NEW 状态的任务
        Iterator<HugeTask<Object>> news = queryTaskWithoutResultByStatus(
            TaskStatus.NEW);

        while (!this.closed.get() && news.hasNext()) {
            HugeTask<?> newTask = news.next();
            LOG.info("Try to start task({})@({}/{})", newTask.id(),
                     this.graphSpace, this.graphName);
            if (!tryStartHugeTask(newTask)) {
                // 任务提交失败时，线程池已打满
                break;
            }
        }

        // 处理 RUNNING 状态的任务
        Iterator<HugeTask<Object>> runnings =
            queryTaskWithoutResultByStatus(TaskStatus.RUNNING);

        while (!this.closed.get() && runnings.hasNext()) {
            HugeTask<?> running = runnings.next();
            initTaskParams(running);
            if (!isLockedTask(running.id().toString())) {
                LOG.info("Try to update task({})@({}/{}) status" +
                         "(RUNNING->FAILED)", running.id(), this.graphSpace,
                         this.graphName);
                if (updateStatusWithLock(running.id(), TaskStatus.RUNNING,
                                         TaskStatus.FAILED)) {
                    runningTasks.remove(running.id());
                } else {
                    LOG.warn("Update task({})@({}/{}) status" +
                             "(RUNNING->FAILED) failed",
                             running.id(), this.graphSpace, this.graphName);
                }
            }
        }

        // 处理 FAILED/HANGING 状态的任务
        Iterator<HugeTask<Object>> faileds =
            queryTaskWithoutResultByStatus(TaskStatus.FAILED);

        while (!this.closed.get() && faileds.hasNext()) {
            HugeTask<?> failed = faileds.next();
            initTaskParams(failed);
            if (failed.retries() < this.graph().option(CoreOptions.TASK_RETRY)) {
                LOG.info("Try to update task({})@({}/{}) status(FAILED->NEW)",
                         failed.id(), this.graphSpace, this.graphName);
                updateStatusWithLock(failed.id(), TaskStatus.FAILED,
                                     TaskStatus.NEW);
            }
        }

        // 处理 CANCELLING 状态的任务
        Iterator<HugeTask<Object>> cancellings = queryTaskWithoutResultByStatus(
            TaskStatus.CANCELLING);

        while (!this.closed.get() && cancellings.hasNext()) {
            Id cancellingId = cancellings.next().id();
            if (runningTasks.containsKey(cancellingId)) {
                HugeTask<?> cancelling = runningTasks.get(cancellingId);
                initTaskParams(cancelling);
                LOG.info("Try to cancel task({})@({}/{})",
                         cancelling.id(), this.graphSpace, this.graphName);
                cancelling.cancel(true);

                runningTasks.remove(cancellingId);
            } else {
                // 本地没有执行任务，但是当前任务已经无节点在执行
                if (!isLockedTask(cancellingId.toString())) {
                    updateStatusWithLock(cancellingId, TaskStatus.CANCELLING,
                                         TaskStatus.CANCELLED);
                }
            }
        }

        // 处理 DELETING 状态的任务
        Iterator<HugeTask<Object>> deletings = queryTaskWithoutResultByStatus(
            TaskStatus.DELETING);

        while (!this.closed.get() && deletings.hasNext()) {
            Id deletingId = deletings.next().id();
            if (runningTasks.containsKey(deletingId)) {
                HugeTask<?> deleting = runningTasks.get(deletingId);
                initTaskParams(deleting);
                deleting.cancel(true);

                // 删除存储信息
                deleteFromDB(deletingId);

                runningTasks.remove(deletingId);
            } else {
                // 本地没有执行任务，但是当前任务已经无节点在执行
                if (!isLockedTask(deletingId.toString())) {
                    deleteFromDB(deletingId);
                }
            }
        }
    }

    protected <V> Iterator<HugeTask<V>> queryTaskWithoutResultByStatus(TaskStatus status) {
        if (this.closed.get()) {
            return QueryResults.emptyIterator();
        }
        return queryTaskWithoutResult(HugeTask.P.STATUS, status.code(), NO_LIMIT, null);
    }

    @Override
    public HugeGraph graph() {
        return this.graph.graph();
    }

    @Override
    public int pendingTasks() {
        return this.runningTasks.size();
    }

    @Override
    public <V> void restoreTasks() {
        // DO Nothing!
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        initTaskParams(task);

        if (task.ephemeralTask()) {
            // 处理 ephemeral 任务，不需要调度，直接执行
            return this.ephemeralTaskExecutor.submit(task);
        }

        // 处理 schema 任务
        // 处理 gremlin 任务
        // 处理 olap 计算任务
        // 添加任务到 DB，当前任务状态为 NEW
        // TODO: save server id for task
        this.save(task);

        if (!this.closed.get()) {
            LOG.info("Try to start task({})@({}/{}) immediately", task.id(),
                     this.graphSpace, this.graphName);
            tryStartHugeTask(task);
        } else {
            LOG.info("TaskScheduler has closed");
        }

        return null;
    }

    protected <V> void initTaskParams(HugeTask<V> task) {
        // 绑定当前任务执行所需的环境变量
        // 在任务反序列化和执行之前，均需要调用该方法
        task.scheduler(this);
        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());

        if (callable instanceof TaskCallable.SysTaskCallable) {
            ((TaskCallable.SysTaskCallable<?>) callable).params(this.graph);
        }
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        // 更新状态为 CANCELLING
        if (!task.completed()) {
            // 任务未完成，才可执行状态未 CANCELLING
            this.updateStatus(task.id(), null, TaskStatus.CANCELLING);
        } else {
            LOG.info("cancel task({}) error, task has completed", task.id());
        }
    }

    @Override
    public void init() {
        this.call(() -> this.tx().initSchema());
    }

    protected <V> HugeTask<V> deleteFromDB(Id id) {
        // 从 DB 中删除 Task，不检查任务状态
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
            HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            HugeTask<V> result = HugeTask.fromVertex(vertex);
            this.tx().removeVertex(vertex);
            return result;
        });
    }

    @Override
    public <V> HugeTask<V> delete(Id id, boolean force) {
        if (!force) {
            // 更改状态为 DELETING，通过自动调度实现删除操作
            this.updateStatus(id, null, TaskStatus.DELETING);
            return null;
        } else {
            return this.deleteFromDB(id);
        }
    }

    @Override
    public boolean close() {
        if (this.closed.get()) {
            return true;
        }

        // set closed
        this.closed.set(true);

        // cancel all running tasks
        for (HugeTask<?> task : this.runningTasks.values()) {
            LOG.info("cancel task({}) @({}/{}) when closing scheduler",
                     task.id(), graphSpace, graphName);
            this.cancel(task);
        }

        try {
            this.waitUntilAllTasksCompleted(10);
        } catch (TimeoutException e) {
            LOG.warn("Tasks not completed when close distributed task scheduler", e);
        }

        // cancel cron thread
        if (!cronFuture.isDone() && !cronFuture.isCancelled()) {
            cronFuture.cancel(false);
        }

        if (!this.taskDbExecutor.isShutdown()) {
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
            });
        }
        return true;
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
        throws TimeoutException {
        return this.waitUntilTaskCompleted(id, seconds, QUERY_INTERVAL);
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id)
        throws TimeoutException {
        // This method is just used by tests
        long timeout = this.graph.configuration()
                                 .get(CoreOptions.TASK_WAIT_TIMEOUT);
        return this.waitUntilTaskCompleted(id, timeout, 1L);
    }

    private <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds,
                                                   long intervalMs)
        throws TimeoutException {
        long passes = seconds * 1000 / intervalMs;
        HugeTask<V> task = null;
        for (long pass = 0; ; pass++) {
            try {
                task = this.taskWithoutResult(id);
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
                // 查询带有结果的任务信息
                task = this.task(id);
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
    public void waitUntilAllTasksCompleted(long seconds)
        throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        int taskSize = 0;
        for (long pass = 0; ; pass++) {
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

    @Override
    public void checkRequirement(String op) {
        if (!this.serverManager().selfIsMaster()) {
            throw new HugeException("Can't %s task on non-master server", op);
        }
    }

    @Override
    public <V> V call(Callable<V> callable) {
        return this.call(callable, this.taskDbExecutor);
    }

    @Override
    public <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    private <V> V call(Callable<V> callable, ExecutorService executor) {
        try {
            callable = new TaskManager.ContextCallable<>(callable);
            return executor.submit(callable).get();
        } catch (Exception e) {
            throw new HugeException("Failed to update/query TaskStore for " +
                                    "graph(%s/%s): %s", e, this.graphSpace,
                                    this.graph.name(), e.toString());
        }
    }

    protected boolean updateStatus(Id id, TaskStatus prestatus,
                                   TaskStatus status) {
        HugeTask<Object> task = this.taskWithoutResult(id);
        initTaskParams(task);
        if (prestatus == null || task.status() == prestatus) {
            task.overwriteStatus(status);
            // 如果状态更新为 FAILED -> NEW，则增加重试次数
            if (prestatus == TaskStatus.FAILED && status == TaskStatus.NEW) {
                task.retry();
            }
            this.save(task);
            LOG.info("Update task({}) success: pre({}), status({})",
                     id, prestatus, status);

            return true;
        } else {
            LOG.warn("Update task({}) status conflict: current({}), " +
                     "pre({}), status({})", id, task.status(),
                     prestatus, status);
            return false;
        }
    }

    protected boolean updateStatusWithLock(Id id, TaskStatus prestatus,
                                           TaskStatus status) {

        LockResult lockResult = tryLockTask(id.asString());

        if (lockResult.lockSuccess()) {
            try {
                return updateStatus(id, prestatus, status);
            } finally {
                unlockTask(id.asString(), lockResult);
            }
        }

        return false;
    }

    /**
     * try to start task;
     *
     * @param task
     * @return true if the task have start
     */
    private boolean tryStartHugeTask(HugeTask<?> task) {
        // Print Scheduler status
        logCurrentState();

        initTaskParams(task);

        ExecutorService chosenExecutor = gremlinTaskExecutor;

        if (task.computer()) {
            chosenExecutor = this.olapTaskExecutor;
        }

        // TODO: uncomment later - vermeer job
        //if (task.vermeer()) {
        //    chosenExecutor = this.olapTaskExecutor;
        //}

        if (task.gremlinTask()) {
            chosenExecutor = this.gremlinTaskExecutor;
        }

        if (task.schemaTask()) {
            chosenExecutor = schemaTaskExecutor;
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) chosenExecutor;
        if (executor.getActiveCount() < executor.getMaximumPoolSize()) {
            TaskRunner<?> runner = new TaskRunner<>(task);
            chosenExecutor.submit(runner);
            LOG.info("Submit task({})@({}/{})", task.id(),
                     this.graphSpace, this.graphName);

            return true;
        }

        return false;
    }

    protected void logCurrentState() {
        int gremlinActive =
            ((ThreadPoolExecutor) gremlinTaskExecutor).getActiveCount();
        int schemaActive =
            ((ThreadPoolExecutor) schemaTaskExecutor).getActiveCount();
        int ephemeralActive =
            ((ThreadPoolExecutor) ephemeralTaskExecutor).getActiveCount();
        int olapActive =
            ((ThreadPoolExecutor) olapTaskExecutor).getActiveCount();

        LOG.info("Current State: gremlinTaskExecutor({}), schemaTaskExecutor" +
                 "({}), ephemeralTaskExecutor({}), olapTaskExecutor({})",
                 gremlinActive, schemaActive, ephemeralActive, olapActive);
    }

    private LockResult tryLockTask(String taskId) {

        LockResult lockResult = new LockResult();

        try {
            lockResult =
                MetaManager.instance().tryLockTask(graphSpace, graphName,
                                                   taskId);
        } catch (Throwable t) {
            LOG.warn(String.format("try to lock task(%s) error", taskId), t);
        }

        return lockResult;
    }

    private void unlockTask(String taskId, LockResult lockResult) {

        try {
            MetaManager.instance().unlockTask(graphSpace, graphName, taskId,
                                              lockResult);
        } catch (Throwable t) {
            LOG.warn(String.format("try to unlock task(%s) error",
                                   taskId), t);
        }
    }

    private boolean isLockedTask(String taskId) {
        return MetaManager.instance().isLockedTask(graphSpace,
                                                   graphName, taskId);
    }

    private class TaskRunner<V> implements Runnable {

        private final HugeTask<V> task;

        public TaskRunner(HugeTask<V> task) {
            this.task = task;
        }

        @Override
        public void run() {
            LockResult lockResult = tryLockTask(task.id().asString());

            initTaskParams(task);
            if (lockResult.lockSuccess() && !task.completed()) {

                LOG.info("Start task({})", task.id());

                TaskManager.setContext(task.context());
                try {
                    // 1. start task can be from schedule() & cronSchedule()
                    // 2. recheck the status of task, in case one same task
                    // called by both methods at same time;
                    HugeTask<Object> queryTask = task(this.task.id());
                    if (queryTask != null &&
                        !TaskStatus.NEW.equals(queryTask.status())) {
                        return;
                    }

                    runningTasks.put(task.id(), task);

                    // 任务执行不会抛出异常，HugeTask 在执行过程中，会捕获异常，并存储到 DB 中
                    task.run();
                } catch (Throwable t) {
                    LOG.warn("exception when execute task", t);
                } finally {
                    runningTasks.remove(task.id());
                    unlockTask(task.id().asString(), lockResult);

                    LOG.info("task({}) finished.", task.id().toString());
                }
            }
        }
    }

    @Override
    public String graphName() {
        return this.graph.name();
    }

    @Override
    public void taskDone(HugeTask<?> task) {
        // DO Nothing
    }
}
