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

package org.apache.hugegraph.memory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.memory.arbitrator.MemoryArbitrator;
import org.apache.hugegraph.memory.arbitrator.MemoryArbitratorImpl;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.pool.impl.QueryMemoryPool;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryManager.class);
    private static final String QUERY_MEMORY_POOL_NAME_PREFIX = "QueryMemoryPool";
    private static final String ARBITRATE_MEMORY_POOL_NAME = "ArbitrateMemoryPool";
    private static final String DELIMINATOR = "_";
    private static final int ARBITRATE_MEMORY_THREAD_NUM = 12;
    // TODO: read it from conf, current 1G
    public static final long MAX_MEMORY_CAPACITY_IN_BYTES = Bytes.GB;
    private final AtomicLong currentMemoryCapacityInBytes =
            new AtomicLong(MAX_MEMORY_CAPACITY_IN_BYTES);
    private final Set<MemoryPool> queryMemoryPools = new CopyOnWriteArraySet<>();
    private final MemoryArbitrator memoryArbitrator;
    private final ExecutorService arbitrateExecutor;
    // TODO: integrated with mingzhen's monitor thread
    // private final Runnable queryGCThread;

    private MemoryManager() {
        this.memoryArbitrator = new MemoryArbitratorImpl();
        this.arbitrateExecutor = ExecutorUtil.newFixedThreadPool(ARBITRATE_MEMORY_THREAD_NUM,
                                                                 ARBITRATE_MEMORY_POOL_NAME);
    }
    
    public MemoryPool addQueryMemoryPool() {
        int count = queryMemoryPools.size();
        String poolName =
                QUERY_MEMORY_POOL_NAME_PREFIX + DELIMINATOR + count + DELIMINATOR +
                System.currentTimeMillis();
        MemoryPool queryPool = new QueryMemoryPool(poolName, this);
        queryMemoryPools.add(queryPool);
        return queryPool;
    }

    public void gcQueryMemoryPool(MemoryPool pool) {
        queryMemoryPools.remove(pool);
        long reclaimedMemory = pool.getAllocatedBytes();
        pool.releaseSelf();
        currentMemoryCapacityInBytes.addAndGet(reclaimedMemory);
    }

    public long triggerLocalArbitration(MemoryPool targetPool, long neededBytes) {
        Future<Long> future =
                arbitrateExecutor.submit(
                        () -> memoryArbitrator.reclaimLocally(targetPool, neededBytes));
        try {
            return future.get(MemoryArbitrator.MAX_WAIT_TIME_FOR_LOCAL_RECLAIM,
                              TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.warn("MemoryManager: arbitration locally for {} timed out", targetPool, e);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("MemoryManager: arbitration locally for {} interrupted or failed",
                         targetPool,
                         e);
        }
        return 0;
    }

    public long triggerGlobalArbitration(MemoryPool requestPool, long neededBytes) {
        Future<Long> future =
                arbitrateExecutor.submit(
                        () -> memoryArbitrator.reclaimGlobally(requestPool, neededBytes));
        try {
            return future.get(MemoryArbitrator.MAX_WAIT_TIME_FOR_GLOBAL_RECLAIM,
                              TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.warn("MemoryManager: arbitration globally for {} timed out", requestPool, e);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("MemoryManager: arbitration globally for {} interrupted or failed",
                         requestPool, e);
        }
        return 0;
    }

    private static class MemoryManagerHolder {

        private static final MemoryManager INSTANCE = new MemoryManager();

        private MemoryManagerHolder() {
            // empty constructor
        }
    }

    public static MemoryManager getInstance() {
        return MemoryManagerHolder.INSTANCE;
    }
}
