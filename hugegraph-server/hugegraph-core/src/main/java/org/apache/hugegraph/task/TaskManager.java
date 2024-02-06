/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.task;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.concurrent.PausableScheduledThreadPool;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public final class TaskManager {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public static final String TASK_WORKER_PREFIX = "task-worker";
    public static final String TASK_WORKER = TASK_WORKER_PREFIX + "-%d";
    public static final String TASK_DB_WORKER = "task-db-worker-%d";
    public static final String SERVER_INFO_DB_WORKER =
                               "server-info-db-worker-%d";
    public static final String TASK_SCHEDULER = "task-scheduler-%d";

    protected static final long SCHEDULE_PERIOD = 1000L; // unit ms
    private static final long TX_CLOSE_TIMEOUT = 30L; // unit s
    private static final int THREADS = 4;
    private static final TaskManager MANAGER = new TaskManager(THREADS);

    private final Map<HugeGraphParams, TaskScheduler> schedulers;

    private final ExecutorService taskExecutor;
    private final ExecutorService taskDbExecutor;
    private final ExecutorService serverInfoDbExecutor;
    private final PausableScheduledThreadPool schedulerExecutor;

    private boolean enableRoleElected = false;

    public static TaskManager instance() {
        return MANAGER;
    }

    private TaskManager(int pool) {
        this.schedulers = new ConcurrentHashMap<>();

        // For execute tasks
        this.taskExecutor = ExecutorUtil.newFixedThreadPool(pool, TASK_WORKER);
        // For save/query task state, just one thread is ok
        this.taskDbExecutor = ExecutorUtil.newFixedThreadPool(
                              1, TASK_DB_WORKER);
        this.serverInfoDbExecutor = ExecutorUtil.newFixedThreadPool(
                                    1, SERVER_INFO_DB_WORKER);
        // For schedule task to run, just one thread is ok
        this.schedulerExecutor = ExecutorUtil.newPausableScheduledThreadPool(
                                 1, TASK_SCHEDULER);
        // Start after 10x period time waiting for HugeGraphServer startup
        this.schedulerExecutor.scheduleWithFixedDelay(this::scheduleOrExecuteJob,
                                                      10 * SCHEDULE_PERIOD,
                                                      SCHEDULE_PERIOD,
                                                      TimeUnit.MILLISECONDS);
    }

    public void addScheduler(HugeGraphParams graph) {
        E.checkArgumentNotNull(graph, "The graph can't be null");

        TaskScheduler scheduler = new StandardTaskScheduler(graph,
                                  this.taskExecutor, this.taskDbExecutor,
                                  this.serverInfoDbExecutor);
        this.schedulers.put(graph, scheduler);
    }

    public void closeScheduler(HugeGraphParams graph) {
        TaskScheduler scheduler = this.schedulers.get(graph);
        if (scheduler != null) {
            /*
             * Synch close+remove scheduler and iterate scheduler, details:
             * 'closeScheduler' should sync with 'scheduleOrExecuteJob'.
             * Because 'closeScheduler' will be called by 'graph.close()' in
             * main thread and there is gap between 'scheduler.close()'
             * (will close graph tx) and 'this.schedulers.remove(graph)'.
             * In this gap 'scheduleOrExecuteJob' may be run in
             * scheduler-db-thread and 'scheduleOrExecuteJob' will reopen
             * graph tx. As a result, graph tx will mistakenly not be closed
             * after 'graph.close()'.
             */
            synchronized (scheduler) {
                if (scheduler.close()) {
                    this.schedulers.remove(graph);
                }
            }
        }

        if (!this.taskExecutor.isTerminated()) {
            this.closeTaskTx(graph);
        }

        if (!this.schedulerExecutor.isTerminated()) {
            this.closeSchedulerTx(graph);
        }
    }

    private void closeTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.taskExecutor, totalThreads,
                                               graph::closeTx, TX_CLOSE_TIMEOUT);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing task tx", e);
        }
    }

    private void closeSchedulerTx(HugeGraphParams graph) {
        final Callable<Void> closeTx = () -> {
            // Do close-tx for current thread
            graph.closeTx();
            // Let other threads run
            Thread.yield();
            return null;
        };
        try {
            this.schedulerExecutor.submit(closeTx).get();
        } catch (Exception e) {
            throw new HugeException("Exception when closing scheduler tx", e);
        }
    }

    public void pauseScheduledThreadPool() {
        this.schedulerExecutor.pauseSchedule();
    }

    public void resumeScheduledThreadPool() {
        this.schedulerExecutor.resumeSchedule();
    }

    public TaskScheduler getScheduler(HugeGraphParams graph) {
        return this.schedulers.get(graph);
    }

    public ServerInfoManager getServerInfoManager(HugeGraphParams graph) {
        StandardTaskScheduler scheduler = (StandardTaskScheduler)
                                          this.getScheduler(graph);
        if (scheduler == null) {
            return null;
        }
        return scheduler.serverManager();
    }

    public void shutdown(long timeout) {
        assert this.schedulers.isEmpty() : this.schedulers.size();

        Throwable ex = null;
        boolean terminated = this.schedulerExecutor.isTerminated();
        final TimeUnit unit = TimeUnit.SECONDS;

        if (!this.schedulerExecutor.isShutdown()) {
            this.schedulerExecutor.shutdown();
            try {
                terminated = this.schedulerExecutor.awaitTermination(timeout,
                                                                     unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.taskExecutor.isShutdown()) {
            this.taskExecutor.shutdown();
            try {
                terminated = this.taskExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.serverInfoDbExecutor.isShutdown()) {
            this.serverInfoDbExecutor.shutdown();
            try {
                terminated = this.serverInfoDbExecutor.awaitTermination(timeout,
                                                                        unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.taskDbExecutor.isShutdown()) {
            this.taskDbExecutor.shutdown();
            try {
                terminated = this.taskDbExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (!terminated) {
            ex = new TimeoutException(timeout + "s");
        }
        if (ex != null) {
            throw new HugeException("Failed to wait for TaskScheduler", ex);
        }
    }

    public int workerPoolSize() {
        return ((ThreadPoolExecutor) this.taskExecutor).getCorePoolSize();
    }

    public int pendingTasks() {
        int size = 0;
        for (TaskScheduler scheduler : this.schedulers.values()) {
            size += scheduler.pendingTasks();
        }
        return size;
    }

    public void enableRoleElection() {
        this.enableRoleElected = true;
    }

    public void onAsRoleMaster() {
        try {
            for (TaskScheduler entry : this.schedulers.values()) {
                StandardTaskScheduler scheduler = (StandardTaskScheduler) entry;
                ServerInfoManager serverInfoManager = scheduler.serverManager();
                serverInfoManager.changeServerRole(NodeRole.MASTER);
            }
        } catch (Throwable e) {
            LOG.error("Exception occurred when change to master role", e);
            throw e;
        }
    }

    public void onAsRoleWorker() {
        try {
            for (TaskScheduler entry : this.schedulers.values()) {
                StandardTaskScheduler scheduler = (StandardTaskScheduler) entry;
                ServerInfoManager serverInfoManager = scheduler.serverManager();
                serverInfoManager.changeServerRole(NodeRole.WORKER);
            }
        } catch (Throwable e) {
            LOG.error("Exception occurred when change to worker role", e);
            throw e;
        }
    }

    protected void notifyNewTask(HugeTask<?> task) {
        Queue<Runnable> queue = ((ThreadPoolExecutor) this.schedulerExecutor)
                                                          .getQueue();
        if (queue.size() <= 1) {
            /*
             * Notify to schedule tasks initiatively when have new task
             * It's OK to not notify again if there are more than one task in
             * queue(like two, one is timer task, one is immediate task),
             * we don't want too many immediate tasks to be inserted into queue,
             * one notify will cause all the tasks to be processed.
             */
            this.schedulerExecutor.submit(this::scheduleOrExecuteJob);
        }
    }

    private void scheduleOrExecuteJob() {
        // Called by scheduler timer
        try {
            for (TaskScheduler entry : this.schedulers.values()) {
                StandardTaskScheduler scheduler = (StandardTaskScheduler) entry;
                // Maybe other thread close&remove scheduler at the same time
                synchronized (scheduler) {
                    this.scheduleOrExecuteJobForGraph(scheduler);
                }
            }
        } catch (Throwable e) {
            LOG.error("Exception occurred when schedule job", e);
        }
    }

    private void scheduleOrExecuteJobForGraph(StandardTaskScheduler scheduler) {
        E.checkNotNull(scheduler, "scheduler");

        ServerInfoManager serverManager = scheduler.serverManager();
        String graph = scheduler.graphName();

        LockUtil.lock(graph, LockUtil.GRAPH_LOCK);
        try {
            /*
             * Skip if:
             * graph is closed (iterate schedulers before graph is closing)
             *  or
             * graph is not initialized(maybe truncated or cleared).
             *
             * If graph is closing by other thread, current thread get
             * serverManager and try lock graph, at the same time other
             * thread deleted the lock-group, current thread would get
             * exception 'LockGroup xx does not exists'.
             * If graph is closed, don't call serverManager.initialized()
             * due to it will reopen graph tx.
             */
            if (!serverManager.graphIsReady()) {
                return;
            }

            // Update server heartbeat
            serverManager.heartbeat();

            /*
             * Master will schedule tasks to suitable servers.
             * Note a Worker may become to a Master, so elected-Master also needs to
             * execute tasks assigned by previous Master when enableRoleElected=true.
             * However, when enableRoleElected=false, a Master is only set by the
             * config assignment, assigned-Master always stays the same state.
             */
            if (serverManager.selfIsMaster()) {
                scheduler.scheduleTasksOnMaster();
                if (!this.enableRoleElected && !serverManager.onlySingleNode()) {
                    // assigned-Master + non-single-node don't need to execute tasks
                    return;
                }
            }

            // Execute queued tasks scheduled to current server
            scheduler.executeTasksOnWorker(serverManager.selfNodeId());

            // Cancel tasks scheduled to current server
            scheduler.cancelTasksOnWorker(serverManager.selfNodeId());
        } finally {
            LockUtil.unlock(graph, LockUtil.GRAPH_LOCK);
        }
    }

    private static final ThreadLocal<String> CONTEXTS = new ThreadLocal<>();

    protected static void setContext(String context) {
        CONTEXTS.set(context);
    }

    protected static void resetContext() {
        CONTEXTS.remove();
    }

    public static String getContext() {
        return CONTEXTS.get();
    }

    public static class ContextCallable<V> implements Callable<V> {

        private final Callable<V> callable;
        private final String context;

        public ContextCallable(Callable<V> callable) {
            E.checkNotNull(callable, "callable");
            this.context = getContext();
            this.callable = callable;
        }

        @Override
        public V call() throws Exception {
            setContext(this.context);
            try {
                return this.callable.call();
            } finally {
                resetContext();
            }
        }
    }
}
