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

import static org.apache.hugegraph.memory.MemoryManager.DELIMINATOR;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.pool.AbstractMemoryPool;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.RoundUtil;
import org.apache.hugegraph.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMemoryPool extends AbstractMemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(QueryMemoryPool.class);
    private static final String TASK_MEMORY_POOL_NAME_PREFIX = "TaskMemoryPool";
    private static final String EXPAND_SELF = "expand self's max capacity";
    private static final String REQUEST_MEMORY = "request to allocate memory";
    // TODO: read from conf
    private static final long QUERY_POOL_MAX_CAPACITY = Bytes.MB * 100;

    public QueryMemoryPool(String poolName, MemoryManager memoryManager) {
        super(null, poolName, MemoryPoolStats.MemoryPoolType.QUERY, memoryManager);
        this.stats.setMaxCapacity(QUERY_POOL_MAX_CAPACITY);
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes, MemoryPool requestingPool) {
        if (isClosed) {
            LOG.warn("[{}] is already closed, will abort this reclaim", this);
            return 0;
        }
        if (this.equals(requestingPool.findRootQueryPool())) {
            return super.tryToReclaimLocalMemoryWithoutLock(neededBytes, requestingPool);
        }
        return super.tryToReclaimLocalMemory(neededBytes, requestingPool);
    }

    @Override
    public MemoryPool addChildPool(String name) {
        int count = this.children.size();
        String poolName =
                TASK_MEMORY_POOL_NAME_PREFIX + DELIMINATOR + name + DELIMINATOR + count +
                DELIMINATOR + System.currentTimeMillis();
        MemoryPool taskMemoryPool = new TaskMemoryPool(this, poolName, this.memoryManager);
        this.children.add(taskMemoryPool);
        LOG.info("QueryPool-{} added task memory pool {}", this, taskMemoryPool);
        return taskMemoryPool;
    }

    @Override
    public long requestMemoryInternal(long bytes, MemoryPool requestingPool) {
        if (this.isClosed) {
            LOG.warn("[{}] is already closed, will abort this request", this);
            return 0;
        }
        long requestedMemoryFromManager = 0;
        long requestedMemoryFromArbitration = 0;
        try {
            if (this.isBeingArbitrated.get()) {
                this.condition.await();
            }
            // 1. check whether self capacity is enough
            if (getMaxCapacityBytes() - stats.getAllocatedBytes() < bytes) {
                // 2.1 if not, first try to acquire memory from manager
                long neededDelta = bytes - (getMaxCapacityBytes() - stats.getAllocatedBytes());
                long managerReturnedMemoryInBytes = tryToExpandSelfCapacity(neededDelta);
                if (managerReturnedMemoryInBytes > 0) {
                    this.stats.setMaxCapacity(getMaxCapacityBytes() + managerReturnedMemoryInBytes);
                    this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + bytes);
                    this.stats.setNumExpands(this.stats.getNumExpands() + 1);
                    requestedMemoryFromManager = bytes;
                } else {
                    // 2.2 if requiring memory from manager failed, call manager to invoke arbitrate
                    requestedMemoryFromArbitration =
                            requestMemoryThroughArbitration(bytes, requestingPool);
                }
            } else {
                // 3. if capacity is enough, check whether manager has enough memory.
                if (this.memoryManager.handleRequestFromQueryPool(bytes, REQUEST_MEMORY) < 0) {
                    // 3.1 if memory manager doesn't have enough memory, call manager to invoke
                    // arbitrate
                    requestedMemoryFromArbitration =
                            requestMemoryThroughArbitration(bytes, requestingPool);
                } else {
                    // 3.2 if memory manager has enough memory, return success
                    this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + bytes);
                    requestedMemoryFromManager = bytes;
                }
            }
            if (requestedMemoryFromManager > 0) {
                this.memoryManager.consumeAvailableMemory(bytes);
            }
            return requestedMemoryFromManager == 0 ? requestedMemoryFromArbitration :
                   requestedMemoryFromManager;
        } catch (InterruptedException e) {
            LOG.error("[{}] Failed to request memory because ", this, e);
            Thread.currentThread().interrupt();
            return 0;
        }
    }

    private long tryToExpandSelfCapacity(long size) {
        LOG.debug("[{}] try to expand its capacity: size={}", this, size);
        long alignedSize = RoundUtil.sizeAlign(size);
        long realNeededSize =
                RoundUtil.roundDelta(getAllocatedBytes(), alignedSize);
        return this.memoryManager.handleRequestFromQueryPool(realNeededSize, EXPAND_SELF);
    }

    private long requestMemoryThroughArbitration(long bytes, MemoryPool requestingPool) {
        LOG.info("[{}] try to request memory from manager through arbitration: size={}", this,
                 bytes);
        long reclaimedBytes =
                this.memoryManager.triggerLocalArbitration(this, bytes, requestingPool);
        if (reclaimedBytes > 0) {
            this.stats.setNumExpands(this.stats.getNumExpands() + 1);
        }
        // 1. if arbitrate successes, update stats and return success
        if (reclaimedBytes - bytes >= 0) {
            // here we don't update capacity & reserved & allocated, because memory is
            // reclaimed from queryPool itself.
            return bytes;
        } else {
            // 2. if still not enough, try to reclaim globally
            long globalArbitrationNeededBytes = bytes - reclaimedBytes;
            long globalReclaimedBytes = this.memoryManager.triggerGlobalArbitration(this,
                                                                                    globalArbitrationNeededBytes);
            reclaimedBytes += globalReclaimedBytes;
            // 3. if memory is enough, update stats and return success
            if (reclaimedBytes - bytes >= 0) {
                // add capacity
                this.stats.setMaxCapacity(this.stats.getMaxCapacity() + globalReclaimedBytes);
                this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + bytes);
                this.stats.setNumExpands(this.stats.getNumExpands() + 1);
                return bytes;
            }
        }
        // 4. if arbitrate fails, return -1, indicating that request failed.
        return -1;
    }
}
