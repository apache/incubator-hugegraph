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

package org.apache.hugegraph.memory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.memory.arbitrator.IMemoryArbitrator;
import org.apache.hugegraph.memory.arbitrator.MemoryArbitrator;
import org.apache.hugegraph.memory.pool.IMemoryPool;
import org.apache.hugegraph.memory.pool.impl.QueryMemoryPool;

public class MemoryManager {

    private static final String QUERY_MEMORY_POOL_NAME_PREFIX = "QueryMemoryPool";
    private static final String DELIMINATOR = "_";
    // TODO: read it from conf, current 1G
    private final AtomicLong currentMemoryCapacityInBytes = new AtomicLong(1000_000_000);
    private final Set<IMemoryPool> queryMemoryPools = new CopyOnWriteArraySet<>();
    private final IMemoryArbitrator memoryArbitrator;
    // TODO: integrated with mingzhen's monitor thread
    // private final Runnable queryGCThread;

    private MemoryManager() {
        this.memoryArbitrator = new MemoryArbitrator();
    }

    public IMemoryPool addQueryMemoryPool() {
        int count = queryMemoryPools.size();
        String poolName =
                QUERY_MEMORY_POOL_NAME_PREFIX + DELIMINATOR + count + DELIMINATOR +
                System.currentTimeMillis();
        IMemoryPool queryPool = new QueryMemoryPool(poolName, this);
        queryMemoryPools.add(queryPool);
        return queryPool;
    }

    public void gcQueryMemoryPool(IMemoryPool pool) {
        queryMemoryPools.remove(pool);
        long reclaimedMemory = pool.getAllocatedBytes();
        pool.releaseSelf();
        currentMemoryCapacityInBytes.addAndGet(reclaimedMemory);
    }

    private static class MemoryManagerHolder {

        private static final MemoryManager INSTANCE = new MemoryManager();

        private MemoryManagerHolder() {
            // empty constructor
        }
    }

    public static MemoryManager getInstance() {
        return MemoryManagerHolder.INSTANCE;
    }
}
