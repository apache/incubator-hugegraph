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

import java.util.Set;
import java.util.TreeSet;

import org.apache.hugegraph.memory.pool.impl.MemoryPoolStats;

public abstract class AbstractMemoryPool implements IMemoryPool {

    private final Set<IMemoryPool> children =
            new TreeSet<>((o1, o2) -> (int) (o2.getFreeBytes() - o1.getFreeBytes()));
    private IMemoryPool parent;
    private MemoryPoolStats stats;

    public AbstractMemoryPool(IMemoryPool parent, String memoryPoolName) {
        this.parent = parent;
        this.stats = new MemoryPoolStats(memoryPoolName);
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes) {
        long totalReclaimedBytes = 0;
        long currentNeededBytes = neededBytes;
        try {
            for (IMemoryPool child : this.children) {
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
    public void releaseSelf() {
        try {
            for (IMemoryPool child : this.children) {
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
    public IMemoryPool getParentPool() {
        return parent;
    }

    @Override
    public String getName() {
        return stats.getMemoryPoolName();
    }

    @Override
    public Set<IMemoryPool> getChildrenPools() {
        return children;
    }
}
