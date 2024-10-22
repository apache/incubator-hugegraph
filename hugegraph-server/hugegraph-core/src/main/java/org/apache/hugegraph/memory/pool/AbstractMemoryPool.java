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

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.pool.impl.MemoryPoolStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMemoryPool implements MemoryPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMemoryPool.class);
    private final Queue<MemoryPool> children =
            new PriorityQueue<>((o1, o2) -> (int) (o2.getFreeBytes() - o1.getFreeBytes()));
    protected final MemoryManager memoryManager;
    protected MemoryPool parent;
    protected MemoryPoolStats stats;

    public AbstractMemoryPool(MemoryPool parent, String memoryPoolName,
                              MemoryManager memoryManager) {
        this.parent = parent;
        this.stats = new MemoryPoolStats(memoryPoolName);
        this.memoryManager = memoryManager;
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes) {
        LOGGER.info("[{}] tryToReclaimLocalMemory: neededBytes={}", this, neededBytes);
        long totalReclaimedBytes = 0;
        long currentNeededBytes = neededBytes;
        try {
            for (MemoryPool child : this.children) {
                long reclaimedMemory = child.tryToReclaimLocalMemory(currentNeededBytes);
                if (reclaimedMemory > 0) {
                    currentNeededBytes -= reclaimedMemory;
                    totalReclaimedBytes += reclaimedMemory;
                    // Reclaim enough memory.
                    if (currentNeededBytes <= 0) {
                        break;
                    }
                }
            }
            LOGGER.info("[{}] has finished to reclaim memory: totalReclaimedBytes={}, " +
                        "neededBytes={}",
                        this,
                        totalReclaimedBytes, neededBytes);
            return totalReclaimedBytes;
        } finally {
            this.stats.setNumShrinks(this.stats.getNumShrinks() + 1);
            this.stats.setAllocatedBytes(
                    this.stats.getAllocatedBytes() - totalReclaimedBytes);
        }
    }

    @Override
    public void gcChildPool(MemoryPool child, boolean force) {
        if (force) {
            child.releaseSelf();
        }
        // reclaim child's memory and update stats
        stats.setAllocatedBytes(
                stats.getAllocatedBytes() - child.getAllocatedBytes());
        stats.setUsedBytes(stats.getUsedBytes() - child.getUsedBytes());
        this.children.remove(child);
        memoryManager.consumeAvailableMemory(-child.getAllocatedBytes());
    }

    /**
     * called when one layer pool is successfully executed and exited.
     */
    @Override
    public void releaseSelf() {
        LOGGER.info("[{}] starts to releaseSelf", this);
        try {
            // update father
            Optional.ofNullable(parent).ifPresent(parent -> parent.gcChildPool(this, false));
            for (MemoryPool child : this.children) {
                gcChildPool(child, true);
            }
            LOGGER.info("[{}] finishes to releaseSelf", this);
        } finally {
            // Make these objs be GCed by JVM quickly.
            this.stats = null;
            this.parent = null;
            this.children.clear();
        }
    }

    @Override
    public Object tryToAcquireMemory(long bytes) {
        // just record how much memory is used(update stats)
        stats.setUsedBytes(stats.getUsedBytes() + bytes);
        stats.setCumulativeBytes(stats.getCumulativeBytes() + bytes);
        return null;
    }

    @Override
    public long getMaxCapacityBytes() {
        return stats.getMaxCapacity();
    }

    @Override
    public long getUsedBytes() {
        return stats.getUsedBytes();
    }

    @Override
    public long getFreeBytes() {
        return stats.getAllocatedBytes() - stats.getUsedBytes();
    }

    @Override
    public long getAllocatedBytes() {
        return stats.getAllocatedBytes();
    }

    @Override
    public MemoryPoolStats getSnapShot() {
        return stats;
    }

    @Override
    public MemoryPool getParentPool() {
        return parent;
    }

    @Override
    public String getName() {
        return stats.getMemoryPoolName();
    }

    @Override
    public String toString() {
        return getSnapShot().toString();
    }
}
