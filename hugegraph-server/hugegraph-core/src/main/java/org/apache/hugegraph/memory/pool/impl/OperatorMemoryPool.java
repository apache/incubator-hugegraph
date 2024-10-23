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

package org.apache.hugegraph.memory.pool.impl;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.allocator.MemoryAllocator;
import org.apache.hugegraph.memory.pool.AbstractMemoryPool;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.MemoryManageUtils;
import org.apache.hugegraph.memory.util.QueryOutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorMemoryPool extends AbstractMemoryPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorMemoryPool.class);
    private final MemoryAllocator memoryAllocator;

    public OperatorMemoryPool(MemoryPool parent, String poolName,
                              MemoryAllocator memoryAllocator, MemoryManager memoryManager) {
        super(parent, poolName, memoryManager);
        this.memoryAllocator = memoryAllocator;
    }

    @Override
    public synchronized void releaseSelf(String reason) {
        memoryAllocator.releaseMemory(getAllocatedBytes());
        super.releaseSelf(reason);
        // TODO: release memory consumer, release byte buffer.
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes) {
        if (isClosed) {
            LOGGER.warn("[{}] is already closed, will abort this reclaim", this);
            return 0;
        }
        LOGGER.info("[{}] tryToReclaimLocalMemory: neededBytes={}", this, neededBytes);
        try {
            this.arbitrationLock.lock();
            this.isBeingArbitrated.set(true);
            // 1. try to reclaim self free memory
            long reclaimableBytes = getFreeBytes();
            // try its best to reclaim memory
            if (reclaimableBytes <= neededBytes) {
                // 2. update stats
                stats.setAllocatedBytes(stats.getUsedBytes());
                LOGGER.info("[{}] has tried its best to reclaim memory: " +
                            "reclaimedBytes={}," +
                            " " +
                            "neededBytes={}",
                            this,
                            reclaimableBytes, neededBytes);
                return reclaimableBytes;
            }
            stats.setAllocatedBytes(stats.getAllocatedBytes() - neededBytes);
            LOGGER.info("[{}] has reclaim enough memory: " +
                        "reclaimedBytes={}," +
                        " " +
                        "neededBytes={}",
                        this,
                        neededBytes, neededBytes);

            return neededBytes;
        } finally {
            this.isBeingArbitrated.set(false);
            this.arbitrationLock.unlock();
            this.condition.signalAll();
        }
    }

    /**
     * called by user
     */
    @Override
    public Object requireMemory(long bytes) {
        try {
            // use lock to ensure the atomicity of the two-step operation
            this.arbitrationLock.lock();
            long realBytes = requestMemoryInternal(bytes);
            return tryToAcquireMemoryInternal(realBytes);
        } catch (QueryOutOfMemoryException e) {
            // Abort this query
            LOGGER.warn("[{}] detected an OOM exception when request memory, will ABORT this " +
                        "query and release corresponding memory...",
                        this);
            findRootQueryPool().releaseSelf(String.format(e.getMessage()));
            return null;
        } finally {
            this.arbitrationLock.unlock();
        }
    }

    /**
     * Operator need `size` bytes, operator pool will try to reserve some memory for it
     */
    @Override
    public Object tryToAcquireMemoryInternal(long size) {
        if (isClosed) {
            LOGGER.warn("[{}] is already closed, will abort this allocate", this);
            return 0;
        }
        LOGGER.info("[{}] tryToAcquireMemory: size={}", this, size);
        // 1. update statistic
        super.tryToAcquireMemoryInternal(size);
        // 2. allocate memory, currently use off-heap mode.
        return memoryAllocator.tryToAllocate(size);
    }

    @Override
    public long requestMemoryInternal(long size) throws QueryOutOfMemoryException {
        if (isClosed) {
            LOGGER.warn("[{}] is already closed, will abort this request", this);
            return 0;
        }
        try {
            if (isBeingArbitrated.get()) {
                condition.await();
            }
            LOGGER.info("[{}] requestMemory: request size={}", this, size);
            // 1. align size
            long alignedSize = MemoryManageUtils.sizeAlign(size);
            // 2. reserve(round)
            long neededMemorySize = calculateReserveMemoryDelta(alignedSize);
            if (neededMemorySize <= 0) {
                return 0;
            }
            // 3. call father
            long fatherRes = getParentPool().requestMemoryInternal(neededMemorySize);
            if (fatherRes < 0) {
                LOGGER.error("[{}] requestMemory failed because of OOM, request size={}", this,
                             size);
                stats.setNumAborts(stats.getNumAborts() + 1);
                throw new QueryOutOfMemoryException(String.format("%s requestMemory failed " +
                                                                  "because of OOM, request " +
                                                                  "size=%s", this, size));
            }
            // 4. update stats
            stats.setAllocatedBytes(stats.getAllocatedBytes() + neededMemorySize);
            stats.setNumExpands(stats.getNumExpands() + 1);
            LOGGER.info("[{}] requestMemory success: requestedMemorySize={}", this, fatherRes);
            return fatherRes;
        } catch (InterruptedException e) {
            LOGGER.error("Failed to release self because ", e);
            Thread.currentThread().interrupt();
            return 0;
        }
    }

    /**
     * This method should be synchronized.
     */
    private synchronized long calculateReserveMemoryDelta(long size) {
        // 1. check whether you need to acquire memory or not
        long neededSize = size - (getFreeBytes());
        // 2. if not needed, return 0
        if (neededSize <= 0) {
            return 0;
        }
        // 3. if needed, calculate rounded size and return it
        return MemoryManageUtils.roundDelta(getAllocatedBytes(), neededSize);
    }
}
