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

package org.apache.hugegraph.memory.arbitrator;

import java.util.Queue;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: consider concurrency
public class MemoryArbitratorImpl implements MemoryArbitrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryArbitratorImpl.class);
    private final MemoryManager memoryManager;

    public MemoryArbitratorImpl(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public long reclaimLocally(MemoryPool queryPool, long neededBytes) {
        long startTime = System.currentTimeMillis();
        // TODO：suspend that task
        long res = queryPool.tryToReclaimLocalMemory(neededBytes);
        LOGGER.info("[{}] reclaim local memory: {} bytes, took {} ms",
                    Thread.currentThread().getName(),
                    res,
                    System.currentTimeMillis() - startTime);
        return res;
    }

    @Override
    public long reclaimGlobally(MemoryPool queryPool, long neededBytes) {
        long startTime = System.currentTimeMillis();
        long totalReclaimedBytes = 0;
        long currentNeededBytes = neededBytes;
        Queue<MemoryPool> currentMemoryPool = memoryManager.getCurrentQueryMemoryPools();
        while (!currentMemoryPool.isEmpty()) {
            MemoryPool memoryPool = currentMemoryPool.poll();
            if (memoryPool.equals(queryPool)) {
                continue;
            }
            LOGGER.info("Global reclaim triggerred by {} select {} to reclaim", queryPool,
                        memoryPool);
            // TODO：suspend that task
            long res = memoryPool.tryToReclaimLocalMemory(currentNeededBytes);
            totalReclaimedBytes += res;
            currentNeededBytes -= res;
            if (currentNeededBytes <= 0) {
                break;
            }
        }
        LOGGER.info("[{}] reclaim global memory: {} bytes, took {} ms",
                    Thread.currentThread().getName(),
                    totalReclaimedBytes,
                    System.currentTimeMillis() - startTime);
        return totalReclaimedBytes;
    }
}
