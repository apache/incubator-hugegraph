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

package org.apache.hugegraph.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hugegraph.concurrent.PausableScheduledThreadPool;



public final class ExecutorUtil {

    public static ThreadPoolExecutor newDynamicThreadExecutor(String name,
                                                              int corePoolSize,
                                                              int maximumPoolSize) {

        long keepAliveTime = 60L;
        TimeUnit unit = TimeUnit.SECONDS;
        ThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern(name)
                .build();
        CustomBlockingQueue<Runnable> workQueue = new CustomBlockingQueue<>();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                workQueue,
                factory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        workQueue.setThreadPoolExecutor(threadPoolExecutor);

        return threadPoolExecutor;
    }

    static class CustomBlockingQueue<E> extends LinkedBlockingQueue<E> {
        private ThreadPoolExecutor threadPoolExecutor;

        public void setThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
            this.threadPoolExecutor = threadPoolExecutor;
        }

        @Override
        public boolean offer(E e) {
            if (threadPoolExecutor.getPoolSize() < threadPoolExecutor.getMaximumPoolSize()) {
                return false;
            }
            return super.offer(e);
        }
    }

    public static ExecutorService newFixedThreadPool(String name) {
        return newFixedThreadPool(1, name);
    }

    public static ExecutorService newFixedThreadPool(int size, String name) {
        ThreadFactory factory = new BasicThreadFactory.Builder()
                                                      .namingPattern(name)
                                                      .build();
        return Executors.newFixedThreadPool(size, factory);
    }

    public static ScheduledExecutorService newScheduledThreadPool(String name) {
        return newScheduledThreadPool(1, name);
    }

    public static ScheduledExecutorService newScheduledThreadPool(int size,
                                                                  String name) {
        ThreadFactory factory = new BasicThreadFactory.Builder()
                                                      .namingPattern(name)
                                                      .build();
        return Executors.newScheduledThreadPool(size, factory);
    }

    public static PausableScheduledThreadPool newPausableScheduledThreadPool(
                                              String name) {
        return newPausableScheduledThreadPool(1, name);
    }

    public static PausableScheduledThreadPool newPausableScheduledThreadPool(
                                              int size, String name) {
        ThreadFactory factory = new BasicThreadFactory.Builder()
                                                      .namingPattern(name)
                                                      .build();
        return new PausableScheduledThreadPool(size, factory);
    }
}
