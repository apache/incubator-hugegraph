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

package org.apache.hugegraph.store.client.util;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

/**
 * 2021/11/22
 */
@Slf4j
public final class ExecutorPool {

    public static ThreadFactory newThreadFactory(String namePrefix, int priority) {
        HgAssert.isArgumentNotNull(namePrefix, "namePrefix");
        return new HgThreadFactory(namePrefix, priority);
    }

    public static ThreadFactory newThreadFactory(String namePrefix) {
        HgAssert.isArgumentNotNull(namePrefix, "namePrefix");
        return new HgDefaultThreadFactory(namePrefix);
    }

    public static ThreadPoolExecutor createExecutor(String name, long keepAliveTime,
                                                    int coreThreads, int maxThreads) {
        return new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS,
                                      new SynchronousQueue<>(),
                                      newThreadFactory(name),
                                      new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    //
    // private final static ExecutorService executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
    //                                                                       10L, TimeUnit.SECONDS,
    //                                                                       new
    //                                                                       LinkedBlockingQueue<>(),
    //                                                                       new
    //                                                                       HgDefaultThreadFactory
    //                                                                       ("store-common"));
    // private final static ExecutorService grpcExecutor = createExecutor("store-grpc", 60L, 600,
    // 10240);
    //// public final static ExecutorService scannerExecutor = createExecutor("scanner", 10l,
    // 200, 1024);
    //
    // static {
    //    Thread hook = new Thread(() -> {
    //        try {
    //            executor.shutdown();
    //            executor.awaitTermination(30, TimeUnit.SECONDS);
    //            return;
    //        } catch (InterruptedException e) {
    //            log.error("failed to await executorService.shutdown()", e);
    //        }
    //        executor.shutdownNow().forEach(r -> log.error("not run task:" + r.toString()));
    //        try {
    //            executor.awaitTermination(30, TimeUnit.SECONDS);
    //        } catch (InterruptedException e) {
    //            log.error("failed to await executorService.shutdownNow()", e);
    //            throw HgStoreClientException.of(e);
    //        }
    //    }
    //    );
    //    Runtime.getRuntime().addShutdownHook(hook);
    //}
    //
    //// public static void execute(Runnable command) {
    ////    isArgumentNotNull(command, "command");
    ////    executor.execute(command);
    //// }
    ////
    // public static ExecutorService getGrpcCommonExecutor() {
    //    return grpcExecutor;
    // }
    //

    /**
     * The default thread factory
     */
    static class HgThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final int priority;

        HgThreadFactory(String namePrefix, int priority) {
            this.namePrefix = namePrefix;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(null, r, namePrefix + "-" + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != priority) {
                t.setPriority(priority);
            }
            return t;
        }
    }

    /**
     * The default thread factory, which added threadNamePrefix in construction method.
     */
    static class HgDefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        HgDefaultThreadFactory(String threadNamePrefix) {
            this.namePrefix = threadNamePrefix + "-" + poolNumber.getAndIncrement() + "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(null, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
