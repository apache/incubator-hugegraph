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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.util.E;

public class HugeTaskManager {

    private static final HugeTaskManager INSTANCE = new HugeTaskManager(4);

    private final Map<HugeGraph, HugeTaskScheduler> schedulers;

    private final ExecutorService taskExecutor;
    private final ExecutorService dbExecutor;

    public static HugeTaskManager instance() {
        return INSTANCE;
    }

    private HugeTaskManager(int pool) {
        this.schedulers = new HashMap<>();

        this.taskExecutor = Executors.newFixedThreadPool(pool);
        this.dbExecutor = Executors.newFixedThreadPool(1);
    }

    public void addScheduler(HugeGraph graph) {
        E.checkArgumentNotNull(graph, "The graph can't be null");
        ExecutorService task = this.taskExecutor;
        ExecutorService db = this.dbExecutor;
        this.schedulers.put(graph, new HugeTaskScheduler(graph, task, db));
    }

    public void closeScheduler(HugeGraph graph) {
        HugeTaskScheduler scheduler = this.schedulers.get(graph);
        if (scheduler != null && scheduler.close()) {
            this.schedulers.remove(graph);
        }
    }

    public HugeTaskScheduler getScheduler(HugeGraph graph) {
        return this.schedulers.get(graph);
    }

    public void shutdown(long timeout) {
        Throwable ex = null;
        assert this.schedulers.isEmpty() : this.schedulers.size();

        if (!this.taskExecutor.isShutdown()) {
            this.taskExecutor.shutdown();
            try {
                this.taskExecutor.awaitTermination(timeout, TimeUnit.SECONDS);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (!this.dbExecutor.isShutdown()) {
            this.dbExecutor.shutdown();
            try {
                this.dbExecutor.awaitTermination(timeout, TimeUnit.SECONDS);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (ex != null) {
            throw new HugeException("Failed to wait for TaskScheduler", ex);
        }
    }
}
