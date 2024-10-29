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

import java.util.HashSet;
import java.util.Set;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.allocator.MemoryAllocator;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.AbstractMemoryPool;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.OutOfMemoryException;
import org.apache.hugegraph.memory.util.RoundUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorMemoryPool extends AbstractMemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorMemoryPool.class);
    private final MemoryAllocator memoryAllocator;
    private final Set<OffHeapObject> offHeapObjects;

    public OperatorMemoryPool(MemoryPool parent, String poolName,
                              MemoryAllocator memoryAllocator, MemoryManager memoryManager) {
        super(parent, poolName, MemoryPoolStats.MemoryPoolType.OPERATOR, memoryManager);
        this.memoryAllocator = memoryAllocator;
        this.offHeapObjects = new HashSet<>();
    }

    @Override
    public MemoryPool addChildPool(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bindMemoryConsumer(OffHeapObject offHeapObject) {
        this.offHeapObjects.add(offHeapObject);
    }

    @Override
    public void releaseSelf(String reason, boolean isTriggeredInternal) {
        super.releaseSelf(reason, isTriggeredInternal);
        // since it is already closed, its stats will not be updated. so here we can use its
        // stats out of memoryActionLock.
        this.memoryAllocator.returnMemoryToManager(getUsedBytes());
        this.memoryManager.returnReclaimedTaskMemory(getAllocatedBytes());
        // release memory consumer, release byte buffer.
        this.offHeapObjects.forEach(memoryConsumer -> {
            memoryConsumer.getAllMemoryBlock().forEach(memoryAllocator::releaseMemoryBlock);
        });
        this.offHeapObjects.clear();
        this.resetStats();
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes, MemoryPool requestingPool) {
        if (this.isClosed) {
            LOG.warn("[{}] is already closed, will abort this reclaim", this);
            return 0;
        }
        long reclaimableBytes = 0;
        try {
            if (!this.equals(requestingPool)) {
                this.memoryActionLock.lock();
            }
            LOG.info("[{}] tryToReclaimLocalMemory: neededBytes={}", this, neededBytes);
            this.isBeingArbitrated.set(true);
            // 1. try to reclaim self free memory
            reclaimableBytes = getFreeBytes();
            // try its best to reclaim memory
            if (reclaimableBytes <= neededBytes) {
                // 2. update stats
                stats.setAllocatedBytes(stats.getUsedBytes());
                LOG.info("[{}] has tried its best to reclaim memory: " +
                         "reclaimedBytes={}," +
                         " " +
                         "neededBytes={}",
                         this,
                         reclaimableBytes, neededBytes);
                return reclaimableBytes;
            }
            stats.setAllocatedBytes(stats.getAllocatedBytes() - neededBytes);
            LOG.info("[{}] has reclaim enough memory: " +
                     "reclaimedBytes={}," +
                     " " +
                     "neededBytes={}",
                     this,
                     neededBytes, neededBytes);

            return neededBytes;
        } finally {
            if (reclaimableBytes > 0) {
                this.stats.setNumShrinks(this.stats.getNumShrinks() + 1);
            }
            this.isBeingArbitrated.set(false);
            if (!this.equals(requestingPool)) {
                this.condition.signalAll();
                this.memoryActionLock.unlock();
            }
        }
    }

    /**
     * called by user
     */
    @Override
    public Object requireMemory(long bytes, MemoryPool requestingPool) {
        try {
            // use lock to ensure the atomicity of the two-step operation
            this.memoryActionLock.lock();
            // if free memory is enough, use free memory directly.
            if (getFreeBytes() >= bytes) {
                this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() - bytes);
            } else {
                // if free memory is not enough, try to request delta
                long delta = bytes - getFreeBytes();
                long ignoredRealAllocatedBytes = requestMemoryInternal(delta, requestingPool);
            }
            return tryToAcquireMemoryInternal(bytes);
        } catch (OutOfMemoryException e) {
            // Abort this query
            LOG.warn("[{}] detected an OOM exception when request memory, will ABORT this " +
                     "query and release corresponding memory...",
                     this);
            findRootQueryPool().releaseSelf(String.format(e.getMessage()), true);
            return null;
        } finally {
            this.memoryActionLock.unlock();
        }
    }

    /**
     * This method will update `used` and `cumulative` stats.
     */
    @Override
    public Object tryToAcquireMemoryInternal(long size) {
        if (this.isClosed) {
            LOG.warn("[{}] is already closed, will abort this allocate", this);
            return null;
        }
        LOG.info("[{}] tryToAcquireMemory: size={}", this, size);
        // 1. update statistic
        super.tryToAcquireMemoryInternal(size);
        // 2. call parent to update statistic
        getParentPool().tryToAcquireMemoryInternal(size);
        // 3. allocate memory, currently use off-heap mode.
        return this.memoryAllocator.tryToAllocate(size);
    }

    /**
     * Operator need `size` bytes, operator pool will try to reserve some memory for it.
     * This method will update `allocated` and `expand` stats.
     */
    @Override
    public long requestMemoryInternal(long size, MemoryPool requestingPool) throws
                                                                            OutOfMemoryException {
        if (this.isClosed) {
            LOG.warn("[{}] is already closed, will abort this request", this);
            return 0;
        }
        try {
            if (this.isBeingArbitrated.get()) {
                this.condition.await();
            }
            LOG.info("[{}] requestMemory: request size={}", this, size);
            // 1. align size
            long alignedSize = RoundUtil.sizeAlign(size);
            // 2. reserve(round)
            long neededMemorySize = calculateReserveMemoryDelta(alignedSize);
            // 3. call father
            long fatherRes =
                    getParentPool().requestMemoryInternal(neededMemorySize, requestingPool);
            if (fatherRes < 0) {
                LOG.error("[{}] requestMemory failed because of OOM, request size={}", this,
                          size);
                this.stats.setNumAborts(this.stats.getNumAborts() + 1);
                throw new OutOfMemoryException(String.format("%s requestMemory failed " +
                                                             "because of OOM, request " +
                                                             "size=%s", this, size));
            }
            // 4. update stats
            this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + neededMemorySize);
            this.stats.setNumExpands(this.stats.getNumExpands() + 1);
            LOG.info("[{}] requestMemory success: requestedMemorySize={}", this, fatherRes);
            return fatherRes;
        } catch (InterruptedException e) {
            LOG.error("Failed to release self because ", e);
            Thread.currentThread().interrupt();
            return 0;
        }
    }

    private long calculateReserveMemoryDelta(long size) {
        return RoundUtil.roundDelta(getAllocatedBytes(), size);
    }

    private void resetStats() {
        this.stats.setNumAborts(0);
        this.stats.setNumExpands(0);
        this.stats.setNumShrinks(0);
        this.stats.setAllocatedBytes(0);
        this.stats.setUsedBytes(0);
        this.stats.setCumulativeBytes(0);
    }
}
