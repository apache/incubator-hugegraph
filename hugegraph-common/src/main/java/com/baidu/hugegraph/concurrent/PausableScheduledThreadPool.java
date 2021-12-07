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

package com.baidu.hugegraph.concurrent;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.Log;

public class PausableScheduledThreadPool extends ScheduledThreadPoolExecutor {

    private static final Logger LOG = Log.logger(
                                      PausableScheduledThreadPool.class);

    private volatile boolean paused = false;

    public PausableScheduledThreadPool(int corePoolSize,
                                       ThreadFactory factory) {
        super(corePoolSize, factory);
    }

    public synchronized void pauseSchedule() {
        this.paused = true;
        LOG.info("PausableScheduledThreadPool was paused");
    }

    public synchronized void resumeSchedule() {
        this.paused = false;
        this.notifyAll();
        LOG.info("PausableScheduledThreadPool was resumed");
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        synchronized (this) {
            while (this.paused) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    LOG.warn("PausableScheduledThreadPool was interrupted");
                }
            }
        }
        super.beforeExecute(t, r);
    }

    @Override
    public void shutdown() {
        if (this.paused) {
            this.resumeSchedule();
        }
        super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        if (this.paused) {
            this.resumeSchedule();
        }
        return super.shutdownNow();
    }
}
