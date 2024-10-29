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

package org.apache.hugegraph.memory.pool;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.impl.MemoryPoolStats;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMemoryPool implements MemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMemoryPool.class);
    protected final Queue<MemoryPool> children =
            new PriorityQueue<>((o1, o2) -> (int) (o2.getFreeBytes() - o1.getFreeBytes()));
    protected final MemoryManager memoryManager;
    // Allocation, deAllocation, arbitration must be serial which is controlled by this lock.
    protected final ReentrantLock memoryActionLock = new ReentrantLock();
    protected final Condition condition = memoryActionLock.newCondition();
    protected final AtomicBoolean isBeingArbitrated = new AtomicBoolean(false);
    protected final MemoryPoolStats stats;
    protected boolean isClosed = false;
    private MemoryPool parent;

    public AbstractMemoryPool(MemoryPool parent, String memoryPoolName,
                              MemoryPoolStats.MemoryPoolType type, MemoryManager memoryManager) {
        this.parent = parent;
        this.stats = new MemoryPoolStats(memoryPoolName, type);
        this.memoryManager = memoryManager;
    }

    protected long tryToReclaimLocalMemoryWithoutLock(long neededBytes, MemoryPool requestingPool) {
        long totalReclaimedBytes = 0;
        try {
            totalReclaimedBytes = reclaimChildren(neededBytes, requestingPool);
            return totalReclaimedBytes;
        } finally {
            if (totalReclaimedBytes > 0) {
                this.stats.setNumShrinks(this.stats.getNumShrinks() + 1);
            }
            this.stats.setAllocatedBytes(
                    this.stats.getAllocatedBytes() - totalReclaimedBytes);
            this.isBeingArbitrated.set(false);
        }
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes, MemoryPool requestingPool) {
        this.memoryActionLock.lock();
        long totalReclaimedBytes = 0;
        try {
            totalReclaimedBytes = reclaimChildren(neededBytes, requestingPool);
            return totalReclaimedBytes;
        } finally {
            if (totalReclaimedBytes > 0) {
                this.stats.setNumShrinks(this.stats.getNumShrinks() + 1);
            }
            this.stats.setAllocatedBytes(
                    this.stats.getAllocatedBytes() - totalReclaimedBytes);
            this.isBeingArbitrated.set(false);
            this.condition.signalAll();
            this.memoryActionLock.unlock();
        }
    }

    private long reclaimChildren(long neededBytes, MemoryPool requestingPool) {
        LOG.info("[{}] tryToReclaimLocalMemory: neededBytes={}", this, neededBytes);
        this.isBeingArbitrated.set(true);
        long totalReclaimedBytes = 0;
        long currentNeededBytes = neededBytes;
        for (MemoryPool child : this.children) {
            long reclaimedMemory =
                    child.tryToReclaimLocalMemory(currentNeededBytes, requestingPool);
            if (reclaimedMemory > 0) {
                currentNeededBytes -= reclaimedMemory;
                totalReclaimedBytes += reclaimedMemory;
                // Reclaim enough memory.
                if (currentNeededBytes <= 0) {
                    break;
                }
            }
        }
        LOG.info("[{}] has finished to reclaim memory: totalReclaimedBytes={}, " +
                 "neededBytes={}",
                 this,
                 totalReclaimedBytes, neededBytes);
        return totalReclaimedBytes;
    }

    /**
     * called when one layer pool is successfully executed and exited.
     */
    @Override
    public void releaseSelf(String reason, boolean isTriggeredInternal) {
        try {
            if (!isTriggeredInternal) {
                this.memoryActionLock.lock();
                if (this.isBeingArbitrated.get()) {
                    this.condition.await();
                }
            }
            LOG.info("[{}] starts to releaseSelf because of {}", this, reason);
            this.isClosed = true;
            // gc self from father
            Optional.ofNullable(this.parent).ifPresent(parent -> parent.gcChildPool(this, false,
                                                                                    isTriggeredInternal));
            // gc all children
            for (MemoryPool child : this.children) {
                gcChildPool(child, true, isTriggeredInternal);
            }
            LOG.info("[{}] finishes to releaseSelf", this);
        } catch (InterruptedException e) {
            LOG.error("Failed to release self because ", e);
            Thread.currentThread().interrupt();
        } finally {
            if (!isTriggeredInternal) {
                this.memoryActionLock.unlock();
            }
            // Make these objs be GCed by JVM quickly.
            this.parent = null;
            this.children.clear();
        }
    }

    @Override
    public void gcChildPool(MemoryPool child, boolean force, boolean isTriggeredInternal) {
        if (force) {
            child.releaseSelf(String.format("[%s] releaseChildPool", this), isTriggeredInternal);
            return;
        }
        // reclaim child's memory and update stats
        this.stats.setAllocatedBytes(
                stats.getAllocatedBytes() - child.getAllocatedBytes());
        this.stats.setUsedBytes(this.stats.getUsedBytes() - child.getUsedBytes());
        this.children.remove(child);
    }

    @Override
    public Object tryToAcquireMemoryInternal(long bytes) {
        if (isClosed) {
            LOG.warn("[{}] is already closed, will abort this allocate", this);
            return 0;
        }
        // just record how much memory is used(update stats)
        this.stats.setUsedBytes(this.stats.getUsedBytes() + bytes);
        this.stats.setCumulativeBytes(this.stats.getCumulativeBytes() + bytes);
        return null;
    }

    @Override
    public void bindMemoryConsumer(OffHeapObject offHeapObject) {
        // default do nothing
    }

    @Override
    public Object requireMemory(long bytes, MemoryPool requestingPool) {
        return null;
    }

    @Override
    public long getMaxCapacityBytes() {
        return Optional.of(this.stats).map(MemoryPoolStats::getMaxCapacity).orElse(0L);
    }

    @Override
    public long getUsedBytes() {
        return Optional.of(this.stats).map(MemoryPoolStats::getUsedBytes).orElse(0L);
    }

    @Override
    public long getFreeBytes() {
        return Optional.of(this.stats)
                       .map(stats -> stats.getAllocatedBytes() - stats.getUsedBytes()).orElse(0L);
    }

    @Override
    public long getAllocatedBytes() {
        return Optional.of(this.stats).map(MemoryPoolStats::getAllocatedBytes).orElse(0L);
    }

    @Override
    public MemoryPoolStats getSnapShot() {
        return this.stats;
    }

    @Override
    public MemoryPool getParentPool() {
        return this.parent;
    }

    @Override
    public String getName() {
        return this.stats.getMemoryPoolName();
    }

    @Override
    public String toString() {
        return getSnapShot().toString();
    }

    @Override
    public MemoryPool findRootQueryPool() {
        if (this.parent == null) {
            return this;
        }
        return getParentPool().findRootQueryPool();
    }

    @Override
    public void setMaxCapacityBytes(long maxCapacityBytes) {
        this.stats.setMaxCapacity(maxCapacityBytes);
    }

    @TestOnly
    public int getChildrenCount() {
        return this.children.size();
    }
}
