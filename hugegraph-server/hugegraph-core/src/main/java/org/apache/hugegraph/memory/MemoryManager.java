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

import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.hugegraph.memory.pool.impl.TaskMemoryPool;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.ExecutorUtil;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class supports memory management for HugeGraph queries.
 * <p>
 * Memory management is divided into three levels: query level, task (thread) level, and operator
 * level. For each new query, the Manager's {@code addQueryMemoryPool} method is called to
 * construct a new queryPool. During query execution, newTaskPool and newOperatorPool are
 * required on demand.
 * <p>
 * Where memory needs to be requested, use {@code getCorrespondingTaskMemoryPool} to get the
 * current taskPool, and use {@code getCurrentWorkingOperatorMemoryPool} to get the working
 * OperatorPool from the taskPool, and use OperatorPool to request memory
 * <p>
 * Note: current MemoryManager doesn't support on-heap management.
 */
public class MemoryManager {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);
    private static final int ARBITRATE_MEMORY_THREAD_NUM = 12;
    private static final String QUERY_MEMORY_POOL_NAME_PREFIX = "QueryMemoryPool";
    private static final String ARBITRATE_MEMORY_POOL_NAME = "ArbitrateMemoryPool";
    public static final String DELIMINATOR = "_";

    public static long MAX_MEMORY_CAPACITY_IN_BYTES = Bytes.GB;
    public static long MAX_MEMORY_CAPACITY_FOR_ONE_QUERY = Bytes.MB * 100;
    // Current available memory = MAX_MEMORY_CAPACITY - sum(allocated bytes)
    private final AtomicLong currentAvailableMemoryInBytes =
            new AtomicLong(MAX_MEMORY_CAPACITY_IN_BYTES);
    private final AtomicLong currentOffHeapAllocatedMemoryInBytes = new AtomicLong(0);
    private final AtomicLong currentOnHeapAllocatedMemoryInBytes = new AtomicLong(0);

    private final Queue<MemoryPool> queryMemoryPools =
            new PriorityQueue<>((o1, o2) -> (int) (o2.getFreeBytes() - o1.getFreeBytes()));
    private final Map<String, TaskMemoryPool> threadName2TaskMemoryPoolMap =
            new ConcurrentHashMap<>();

    private final MemoryArbitrator memoryArbitrator;
    private final ExecutorService arbitrateExecutor;

    private static MemoryMode MEMORY_MODE = MemoryMode.ENABLE_OFF_HEAP_MANAGEMENT;

    private MemoryManager() {
        this.memoryArbitrator = new MemoryArbitratorImpl(this);
        // Since there is always only 1 working operator pool for 1 query, It is not possible to
        // run local arbitration or global arbitration in parallel within a query. The thread
        // pool here is to allow parallel arbitration between queries
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
        LOG.info("Manager added query memory pool {}", queryPool);
        return queryPool;
    }

    public void gcQueryMemoryPool(MemoryPool pool) {
        LOG.info("Manager gc query memory pool {}", pool);
        queryMemoryPools.remove(pool);
        pool.releaseSelf(String.format("GC query memory pool %s", pool), false);
    }

    public void returnReclaimedTaskMemory(long bytes) {
        currentAvailableMemoryInBytes.addAndGet(bytes);
    }

    public void consumeAvailableMemory(long size) {
        currentAvailableMemoryInBytes.addAndGet(-size);
    }

    public long triggerLocalArbitration(MemoryPool targetPool, long neededBytes,
                                        MemoryPool requestPool) {
        LOG.info("LocalArbitration triggered by {}: needed bytes={}", targetPool, neededBytes);
        Future<Long> future =
                arbitrateExecutor.submit(
                        () -> memoryArbitrator.reclaimLocally(targetPool, neededBytes,
                                                              requestPool));
        try {
            return future.get(MemoryArbitrator.MAX_WAIT_TIME_FOR_LOCAL_RECLAIM,
                              TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOG.warn("MemoryManager: arbitration locally for {} timed out", targetPool, e);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("MemoryManager: arbitration locally for {} interrupted or failed",
                      targetPool,
                      e);
        }
        return 0;
    }

    public long triggerGlobalArbitration(MemoryPool requestPool, long neededBytes) {
        LOG.info("GlobalArbitration triggered by {}: needed bytes={}", requestPool, neededBytes);
        Future<Long> future =
                arbitrateExecutor.submit(
                        () -> memoryArbitrator.reclaimGlobally(requestPool, neededBytes));
        try {
            return future.get(MemoryArbitrator.MAX_WAIT_TIME_FOR_GLOBAL_RECLAIM,
                              TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOG.warn("MemoryManager: arbitration globally for {} timed out", requestPool, e);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("MemoryManager: arbitration globally for {} interrupted or failed",
                      requestPool, e);
        }
        return 0;
    }

    public synchronized long handleRequestFromQueryPool(long size, String action) {
        if (currentAvailableMemoryInBytes.get() < size) {
            LOG.info("There isn't enough memory for query pool to {}: " +
                     "requestSize={}, remainingCapacity={}", size, action,
                     currentAvailableMemoryInBytes.get());
            return -1;
        }
        return size;
    }

    /**
     * Used by task thread to find its memory pool to release self's memory resource when exiting.
     */
    public MemoryPool getCorrespondingTaskMemoryPool(String threadName) {
        return threadName2TaskMemoryPoolMap.getOrDefault(threadName, null);
    }

    public void bindCorrespondingTaskMemoryPool(String threadName, TaskMemoryPool memoryPool) {
        threadName2TaskMemoryPoolMap.computeIfAbsent(threadName, key -> memoryPool);
    }

    public void removeCorrespondingTaskMemoryPool(String threadName) {
        threadName2TaskMemoryPoolMap.remove(threadName);
    }

    public Queue<MemoryPool> getCurrentQueryMemoryPools() {
        return new PriorityQueue<>(queryMemoryPools);
    }

    public AtomicLong getCurrentOnHeapAllocatedMemoryInBytes() {
        return currentOnHeapAllocatedMemoryInBytes;
    }

    public AtomicLong getCurrentOffHeapAllocatedMemoryInBytes() {
        return currentOffHeapAllocatedMemoryInBytes;
    }

    public static void setMemoryMode(MemoryMode conf) {
        MEMORY_MODE = conf;
    }

    public static MemoryMode getMemoryMode() {
        return MEMORY_MODE;
    }

    public static void setMaxMemoryCapacityInBytes(long maxMemoryCapacityInBytes) {
        MAX_MEMORY_CAPACITY_IN_BYTES = maxMemoryCapacityInBytes;
    }

    public static void setMaxMemoryCapacityForOneQuery(long maxMemoryCapacityForOneQuery) {
        MAX_MEMORY_CAPACITY_FOR_ONE_QUERY = maxMemoryCapacityForOneQuery;
    }

    @TestOnly
    public AtomicLong getCurrentAvailableMemoryInBytes() {
        return currentAvailableMemoryInBytes;
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

    public enum MemoryMode {
        ENABLE_OFF_HEAP_MANAGEMENT("off-heap"),
        ENABLE_ON_HEAP_MANAGEMENT("on-heap"),
        DISABLE_MEMORY_MANAGEMENT("disable");

        private final String value;

        MemoryMode(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static MemoryMode fromValue(String value) {
            if (value.equalsIgnoreCase(ENABLE_ON_HEAP_MANAGEMENT.getValue())) {
                return ENABLE_ON_HEAP_MANAGEMENT;
            } else if (value.equalsIgnoreCase(ENABLE_OFF_HEAP_MANAGEMENT.getValue())) {
                return ENABLE_OFF_HEAP_MANAGEMENT;
            }
            // return DISABLE by default
            return DISABLE_MEMORY_MANAGEMENT;
        }
    }
}
