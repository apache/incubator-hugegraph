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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ExecutorUtil;

public class TaskManager {

    public static final String TASK_WORKER = "task-worker-%d";
    public static final String TASK_DB_WORKER = "task-db-worker-%d";

    private static final int THREADS = 4;
    private static final TaskManager MANAGER = new TaskManager(THREADS);

    private final Map<HugeGraphParams, TaskScheduler> schedulers;

    private final ExecutorService taskExecutor;
    private final ExecutorService dbExecutor;

    public static TaskManager instance() {
        return MANAGER;
    }

    private TaskManager(int pool) {
        this.schedulers = new HashMap<>();

        // For execute tasks
        this.taskExecutor = ExecutorUtil.newFixedThreadPool(pool, TASK_WORKER);
        // For save/query task state, just one thread is ok
        this.dbExecutor = ExecutorUtil.newFixedThreadPool(1, TASK_DB_WORKER);
    }

    public void addScheduler(HugeGraphParams graph) {
        E.checkArgumentNotNull(graph, "The graph can't be null");
        ExecutorService task = this.taskExecutor;
        ExecutorService db = this.dbExecutor;
        this.schedulers.put(graph, new TaskScheduler(graph, task, db));
    }

    public void closeScheduler(HugeGraphParams graph) {
        TaskScheduler scheduler = this.schedulers.get(graph);
        if (scheduler != null && scheduler.close()) {
            this.schedulers.remove(graph);
        }
        if (!this.taskExecutor.isTerminated()) {
            this.closeTaskTx(graph);
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

    public TaskScheduler getScheduler(HugeGraphParams graph) {
        return this.schedulers.get(graph);
    }

    public void shutdown(long timeout) {
        assert this.schedulers.isEmpty() : this.schedulers.size();

        Throwable ex = null;
        boolean terminated = this.taskExecutor.isTerminated();
        final TimeUnit unit = TimeUnit.SECONDS;

        if (!this.taskExecutor.isShutdown()) {
            this.taskExecutor.shutdown();
            try {
                terminated = this.taskExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.dbExecutor.isShutdown()) {
            this.dbExecutor.shutdown();
            try {
                terminated = this.dbExecutor.awaitTermination(timeout, unit);
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
}
