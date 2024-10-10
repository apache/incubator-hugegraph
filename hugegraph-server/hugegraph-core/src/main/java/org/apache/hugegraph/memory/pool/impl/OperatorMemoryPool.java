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

import org.apache.hugegraph.memory.allocator.MemoryAllocator;
import org.apache.hugegraph.memory.pool.AbstractMemoryPool;
import org.apache.hugegraph.memory.pool.MemoryPool;

public class OperatorMemoryPool extends AbstractMemoryPool {

    // TODO: configurable
    private static final long ALIGNMENT = 8;
    private static final long MB = 1 << 20;
    // TODO: implement different allocate strategy & make it configurable.
    private final MemoryAllocator memoryAllocator;

    public OperatorMemoryPool(MemoryPool parent, String poolName,
                              MemoryAllocator memoryAllocator) {
        super(parent, poolName);
        this.memoryAllocator = memoryAllocator;
        // TODO: this.stats.setMaxCapacity();
    }

    @Override
    public boolean tryToDiskSpill() {
        return false;
    }

    @Override
    public void releaseSelf() {
        super.releaseSelf();
        // TODO: release memory consumer, release byte buffer.
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes) {
        // 1. try to reclaim self free memory
        long reclaimableBytes = getFreeBytes();
        // try its best to reclaim memory
        if (reclaimableBytes <= neededBytes) {
            // 2. update stats
            stats.setAllocatedBytes(stats.getUsedBytes());
            stats.setReservedBytes(stats.getUsedBytes());
            return reclaimableBytes;
        }
        stats.setAllocatedBytes(stats.getAllocatedBytes() - neededBytes);
        stats.setReservedBytes(stats.getReservedBytes() - neededBytes);
        return neededBytes;
    }

    /**
     * Operator need `size` bytes, operator pool will try to reserve some memory for it
     */
    @Override
    public Object tryToAcquireMemory(long size) {
        // 1. update statistic
        super.tryToAcquireMemory(size);
        // 2. allocate memory, currently use off-heap mode.
        // if you use on-heap mode, we only track memory usage here.
        return memoryAllocator.tryToAllocateOffHeap(size);
    }

    @Override
    public long requestMemory(long size) {
        // TODO: check max capacity
        // 1. align size
        long alignedSize = sizeAlign(size);
        // 2. reserve(round)
        long neededMemorySize = calculateReserveMemoryDelta(alignedSize);
        if (neededMemorySize <= 0) {
            return 0;
        }
        // 3. call father
        long fatherRes = getParentPool().requestMemory(neededMemorySize);
        if (fatherRes < 0) {
            // TODO: new OOM exception
            stats.setNumAborts(stats.getNumAborts() + 1);
            throw new OutOfMemoryError();
        }
        // 4. update stats
        stats.setReservedBytes(stats.getReservedBytes() + neededMemorySize);
        stats.setAllocatedBytes(stats.getAllocatedBytes() + neededMemorySize);
        stats.setNumExpands(stats.getNumExpands() + 1);
        return fatherRes;
    }

    private long sizeAlign(long size) {
        long reminder = size % ALIGNMENT;
        return reminder == 0 ? size : size + ALIGNMENT - reminder;
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
        return roundDelta(stats.getReservedBytes(), neededSize);
    }

    private long roundDelta(long reservedSize, long delta) {
        return quantizedSize(reservedSize + delta) - reservedSize;
    }

    private long quantizedSize(long size) {
        if (size < 16 * MB) {
            return roundUp(size, MB);
        }
        if (size < 64 * MB) {
            return roundUp(size, 4 * MB);
        }
        return roundUp(size, 8 * MB);
    }

    private long roundUp(long size, long factor) {
        return (size + factor - 1) / factor * factor;
    }
}
