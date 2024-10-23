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
import org.apache.hugegraph.memory.pool.AbstractMemoryPool;
import org.apache.hugegraph.memory.util.MemoryManageUtils;
import org.apache.hugegraph.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMemoryPool extends AbstractMemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(QueryMemoryPool.class);
    // TODO: read from conf
    private static final long QUERY_POOL_MAX_CAPACITY = Bytes.MB * 100;

    public QueryMemoryPool(String poolName, MemoryManager memoryManager) {
        super(null, poolName, memoryManager);
        this.stats.setMaxCapacity(QUERY_POOL_MAX_CAPACITY);
    }

    @Override
    public long requestMemoryInternal(long bytes) {
        if (this.isClosed) {
            LOG.warn("[{}] is already closed, will abort this request", this);
            return 0;
        }
        try {
            if (this.isBeingArbitrated.get()) {
                this.condition.await();
            }
            // 1. check whether self capacity is enough
            if (getMaxCapacityBytes() - stats.getAllocatedBytes() < bytes) {
                // 2.1 if not, first try to acquire memory from manager
                long managerReturnedMemoryInBytes = tryToExpandSelfCapacity(bytes);
                if (managerReturnedMemoryInBytes > 0) {
                    this.stats.setMaxCapacity(getMaxCapacityBytes() + managerReturnedMemoryInBytes);
                    this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + bytes);
                    this.stats.setNumExpands(this.stats.getNumExpands() + 1);
                    this.memoryManager.consumeAvailableMemory(bytes);
                    return bytes;
                }
                // 2.2 if requiring memory from manager failed, call manager to invoke arbitrate
                // locally
                return requestMemoryThroughArbitration(bytes);
            } else {
                // 3. if capacity is enough, return success
                this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + bytes);
                this.memoryManager.consumeAvailableMemory(bytes);
                return bytes;
            }
        } catch (InterruptedException e) {
            LOG.error("Failed to release self because ", e);
            Thread.currentThread().interrupt();
            return 0;
        }
    }

    private long tryToExpandSelfCapacity(long size) {
        LOG.info("[{}] try to expand its capacity: size={}", this, size);
        long alignedSize = MemoryManageUtils.sizeAlign(size);
        long realNeededSize =
                MemoryManageUtils.roundDelta(getAllocatedBytes(), alignedSize);
        return this.memoryManager.handleRequestFromQueryPool(realNeededSize);
    }

    private long requestMemoryThroughArbitration(long bytes) {
        LOG.info("[{}] try to request memory from manager through arbitration: size={}", this,
                 bytes);
        this.stats.setNumExpands(this.stats.getNumExpands() + 1);
        long reclaimedBytes = this.memoryManager.triggerLocalArbitration(this, bytes);
        // 1. if arbitrate successes, update stats and return success
        if (reclaimedBytes - bytes >= 0) {
            // here we don't update capacity & reserved & allocated, because memory is
            // reclaimed from queryPool itself.
            this.memoryManager.consumeAvailableMemory(bytes);
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
                this.memoryManager.consumeAvailableMemory(bytes);
                return bytes;
            }
        }
        // 4. if arbitrate fails, return -1, indicating that request failed.
        return -1;
    }
}
