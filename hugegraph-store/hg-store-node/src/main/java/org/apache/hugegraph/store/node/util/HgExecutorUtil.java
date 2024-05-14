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

package org.apache.hugegraph.store.node.util;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

/**
 * 2022/03/04
 */
@Slf4j
public final class HgExecutorUtil {

    private final static Map<String, ThreadPoolExecutor> threadPoolMap = new ConcurrentHashMap<>();

    public static ThreadFactory newThreadFactory(String namePrefix) {
        return new HgDefaultThreadFactory(namePrefix);
    }

    public static ThreadPoolExecutor getThreadPoolExecutor(String name) {
        if (name == null) {
            return null;
        }
        return threadPoolMap.get(name);
    }

    public static ThreadPoolExecutor createExecutor(String name, int coreThreads, int maxThreads,
                                                    int queueSize) {
        ThreadPoolExecutor res = threadPoolMap.get(name);
        if (res != null) {
            return res;
        }
        synchronized (threadPoolMap) {
            res = threadPoolMap.get(name);
            if (res != null) {
                return res;
            }
            BlockingQueue queue;
            if (queueSize <= 0) {
                queue = new SynchronousQueue();
            } else {
                queue = new LinkedBlockingQueue<>(queueSize);
            }
            res = new ThreadPoolExecutor(coreThreads, maxThreads, 60L, TimeUnit.SECONDS, queue,
                                         newThreadFactory(name));
            threadPoolMap.put(name, res);
        }
        return res;
    }

    /**
     * The default thread factory, which added threadNamePrefix in construction method.
     */
    static class HgDefaultThreadFactory implements ThreadFactory {

        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        HgDefaultThreadFactory(String threadNamePrefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = threadNamePrefix + "-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
