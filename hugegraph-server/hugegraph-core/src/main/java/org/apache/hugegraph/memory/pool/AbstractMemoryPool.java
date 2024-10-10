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

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hugegraph.memory.pool.impl.MemoryPoolStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMemoryPool implements MemoryPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMemoryPool.class);
    private final Set<MemoryPool> children =
            new TreeSet<>((o1, o2) -> (int) (o2.getFreeBytes() - o1.getFreeBytes()));
    private MemoryPool parent;
    protected MemoryPoolStats stats;

    public AbstractMemoryPool(MemoryPool parent, String memoryPoolName) {
        this.parent = parent;
        this.stats = new MemoryPoolStats(memoryPoolName);
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes) {
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
            return totalReclaimedBytes;
        } finally {
            this.stats.setNumShrinks(this.stats.getNumShrinks() + 1);
            this.stats.setAllocatedBytes(
                    this.stats.getAllocatedBytes() - totalReclaimedBytes);
        }
    }

    @Override
    public boolean tryToDiskSpill() {
        // 1. for every child, invoke disk spill
        boolean res = true;
        try {
            for (MemoryPool child : this.children) {
                res &= child.tryToDiskSpill();
            }
            return res;
        } catch (Exception e) {
            LOGGER.error("Failed to try to disk spill", e);
            return false;
        }
        // TODO: for upper caller, if spill failed, apply rollback
    }

    /**
     * called when one task is successfully executed and exited.
     */
    @Override
    public void gcChildPool(MemoryPool child) {
        // 1. reclaim child's memory and update stats
        stats.setReservedBytes(stats.getReservedBytes() - child.getSnapShot().getReservedBytes());
        // 2. release child
        child.releaseSelf();
    }

    @Override
    public void releaseSelf() {
        try {
            for (MemoryPool child : this.children) {
                child.releaseSelf();
            }
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
    public Set<MemoryPool> getChildrenPools() {
        return children;
    }

    @Override
    public String toString() {
        return getSnapShot().toString();
    }
}
