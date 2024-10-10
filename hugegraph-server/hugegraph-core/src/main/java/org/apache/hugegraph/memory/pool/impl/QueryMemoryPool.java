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

public class QueryMemoryPool extends AbstractMemoryPool {

    private final MemoryManager memoryManager;

    public QueryMemoryPool(String poolName, MemoryManager memoryManager) {
        super(null, poolName);
        this.memoryManager = memoryManager;
        // TODO: this.stats.setMaxCapacity();
    }

    @Override
    public long requestMemory(long bytes) {
        // 1. check whether self capacity is enough
        if (getMaxCapacityBytes() - stats.getAllocatedBytes() < bytes) {
            // 2. if not, call manager to invoke arbitrate locally
            long reclaimedBytes = memoryManager.triggerLocalArbitration(this, bytes);
            // 3. if arbitrate successes, update stats and return success
            if (reclaimedBytes - bytes >= 0) {
                // here we don't update capacity, because memory is reclaimed from queryPool itself.
                stats.setReservedBytes(stats.getReservedBytes() + bytes);
                stats.setAllocatedBytes(stats.getAllocatedBytes() + bytes);
                stats.setNumExpands(stats.getNumExpands() + 1);
                return bytes;
            } else {
                // 4. if still not enough, try to reclaim globally
                long globalReclaimedBytes = memoryManager.triggerGlobalArbitration(this,
                                                                                   bytes -
                                                                                   reclaimedBytes);
                reclaimedBytes += globalReclaimedBytes;
                // 5. if memory is enough, update stats and return success
                if (reclaimedBytes - bytes >= 0) {
                    // add capacity
                    stats.setMaxCapacity(stats.getMaxCapacity() + globalReclaimedBytes);
                    stats.setReservedBytes(stats.getReservedBytes() + bytes);
                    stats.setAllocatedBytes(stats.getAllocatedBytes() + bytes);
                    stats.setNumExpands(stats.getNumExpands() + 1);
                    return bytes;
                }
            }
            // 6. if arbitrate fails, return -1, indicating that request failed.
            return -1;
        } else {
            // 7. if capacity is enough, return success
            stats.setReservedBytes(stats.getReservedBytes() + bytes);
            stats.setAllocatedBytes(stats.getAllocatedBytes() + bytes);
            return bytes;
        }
    }

}
