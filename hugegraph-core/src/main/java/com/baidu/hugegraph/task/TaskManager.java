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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public final class TaskManager {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public static final String TASK_WORKER = "task-worker-%d";
    public static final String TASK_DB_WORKER = "task-db-worker-%d";
    public static final String SERVER_INFO_DB_WORKER =
                               "server-info-db-worker-%d";
    public static final String TASK_SCHEDULER = "task-scheduler-%d";

    public static final int SCHEDULE_PERIOD = 3; // Unit second
    private static final int THREADS = 4;
    private static final TaskManager MANAGER = new TaskManager(THREADS);

    private final Map<HugeGraphParams, TaskScheduler> schedulers;
    private final Map<HugeGraphParams, ServerInfoManager> serverInfoManagers;

    private final ExecutorService taskExecutor;
    private final ExecutorService taskDbExecutor;
    private final ExecutorService serverInfoDbExecutor;
    private final ScheduledExecutorService schedulerExecutor;

    public static TaskManager instance() {
        return MANAGER;
    }

    private TaskManager(int pool) {
        this.schedulers = new ConcurrentHashMap<>();
        this.serverInfoManagers = new ConcurrentHashMap<>();

        // For execute tasks
        this.taskExecutor = ExecutorUtil.newFixedThreadPool(pool, TASK_WORKER);
        // For save/query task state, just one thread is ok
        this.taskDbExecutor = ExecutorUtil.newFixedThreadPool(
                              1, TASK_DB_WORKER);
        this.serverInfoDbExecutor = ExecutorUtil.newFixedThreadPool(
                                    1, SERVER_INFO_DB_WORKER);
        // For schedule task to run, just one thread is ok
        this.schedulerExecutor = ExecutorUtil.newScheduledThreadPool(
                                 1, TASK_SCHEDULER);
        // Start after 10s waiting for HugeGraphServer startup
        this.schedulerExecutor.scheduleWithFixedDelay(this::scheduleOrExecuteJob,
                                                      10L, SCHEDULE_PERIOD,
                                                      TimeUnit.SECONDS);
    }

    public void addScheduler(HugeGraphParams graph) {
        E.checkArgumentNotNull(graph, "The graph can't be null");

        TaskScheduler scheduler = new StandardTaskScheduler(
                                  graph, this.taskExecutor,
                                  this.taskDbExecutor);
        this.schedulers.put(graph, scheduler);

        ServerInfoManager serverInfoManager = new ServerInfoManager(
                                              graph, this.serverInfoDbExecutor);
        this.serverInfoManagers.put(graph, serverInfoManager);
    }

    public void closeScheduler(HugeGraphParams graph) {
        TaskScheduler scheduler = this.schedulers.get(graph);
        if (scheduler != null && scheduler.close()) {
            this.schedulers.remove(graph);
        }
        if (!this.taskExecutor.isTerminated()) {
            this.closeTaskTx(graph);
        }

        ServerInfoManager manager = this.serverInfoManagers.get(graph);
        if (manager != null && manager.close()) {
            this.serverInfoManagers.remove(graph);
        }
        if (!this.schedulerExecutor.isTerminated()) {
            this.closeSchedulerTx(graph);
        }
    }

    private void closeTaskTx(HugeGraphParams graph) {
        final Map<Thread, Integer> threadsTimes = new ConcurrentHashMap<>();
        final List<Callable<Void>> tasks = new ArrayList<>();

        final Callable<Void> closeTx = () -> {
            Thread current = Thread.currentThread();
            threadsTimes.putIfAbsent(current, 0);
            int times = threadsTimes.get(current);
            if (times == 0) {
                // Do close-tx for current thread
                graph.closeTx();
                // Let other threads run
                Thread.yield();
            } else {
                assert times < THREADS;
                assert threadsTimes.size() < THREADS;
                E.checkState(tasks.size() == THREADS,
                             "Bad tasks size: %s", tasks.size());
                // Let another thread run and wait for it
                this.taskExecutor.invokeAny(tasks.subList(0, 1));
            }
            threadsTimes.put(current, ++times);
            return null;
        };

        // NOTE: expect each thread to perform a close operation
        for (int i = 0; i < THREADS; i++) {
            tasks.add(closeTx);
        }
        try {
            this.taskExecutor.invokeAll(tasks);
        } catch (Exception e) {
            throw new HugeException("Exception when closing task tx", e);
        }
    }

    private void closeSchedulerTx(HugeGraphParams graph) {
        final Callable<Void> closeTx = () -> {
            graph.closeTx();
            return null;
        };

        try {
            this.schedulerExecutor.submit(closeTx);
        } catch (Exception e) {
            throw new HugeException("Exception when closing scheduler tx", e);
        }
    }

    public TaskScheduler getScheduler(HugeGraphParams graph) {
        return this.schedulers.get(graph);
    }

    public ServerInfoManager getServerInfoManager(HugeGraphParams graph) {
        return this.serverInfoManagers.get(graph);
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

    protected void notifyNewTask(HugeTask<?> task) {
        // Notify to schedule tasks initiatively when have new task
        this.schedulerExecutor.submit(this::scheduleOrExecuteJob);
    }

    private void scheduleOrExecuteJob() {
        try {
            for (Map.Entry<HugeGraphParams, TaskScheduler> entry :
                 this.schedulers.entrySet()) {
                ServerInfoManager server = entry.getKey().serverManager();
                StandardTaskScheduler scheduler = (StandardTaskScheduler)
                                                  entry.getValue();

                // Update server heartbeat
                server.heartbeat();

                /*
                 * Master schedule tasks to suitable servers.
                 * There is no suitable server when these tasks are created
                 */
                if (server.master()) {
                    scheduler.scheduleTasks();
                }

                // Schedule queued tasks scheduled to current server
                scheduler.executeTasksForWorker(server.serverId());

                // Cancel tasks scheduled to current server
                scheduler.cancelTasksForWorker(server.serverId());
            }
        } catch (Throwable e) {
            LOG.error("Exception occurred when schedule job", e);
        }
    }

    private static final ThreadLocal<String> contexts = new ThreadLocal<>();

    protected static final void setContext(String context) {
        contexts.set(context);
    }

    protected static final void resetContext() {
        contexts.remove();
    }

    public static final String getContext() {
        return contexts.get();
    }

    protected static class ContextCallable<V> implements Callable<V> {

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
