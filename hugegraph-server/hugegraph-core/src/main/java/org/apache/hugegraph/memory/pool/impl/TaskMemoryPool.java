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
import org.apache.hugegraph.memory.allocator.NettyMemoryAllocator;
import org.apache.hugegraph.memory.pool.AbstractMemoryPool;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.QueryOutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskMemoryPool extends AbstractMemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMemoryPool.class);
    private static final String OPERATOR_MEMORY_POOL_NAME_PREFIX = "OperatorMemoryPool";

    public TaskMemoryPool(MemoryPool parent, String poolName, MemoryManager memoryManager) {
        super(parent, poolName, memoryManager);
    }

    @Override
    public synchronized void releaseSelf(String reason) {
        this.memoryManager.removeCorrespondingTaskMemoryPool(Thread.currentThread().getName());
        super.releaseSelf(reason);
    }

    @Override
    public MemoryPool addChildPool() {
        int count = this.children.size();
        String poolName =
                OPERATOR_MEMORY_POOL_NAME_PREFIX + DELIMINATOR + count + DELIMINATOR +
                System.currentTimeMillis();
        MemoryPool operatorPool =
                new OperatorMemoryPool(this, poolName,
                                       new NettyMemoryAllocator(this.memoryManager),
                                       this.memoryManager);
        this.children.add(operatorPool);
        LOG.info("TaskPool-{} added operator memory pool {}", this, operatorPool);
        return operatorPool;
    }

    @Override
    public long requestMemoryInternal(long bytes) throws QueryOutOfMemoryException {
        if (this.isClosed) {
            LOG.warn("[{}] is already closed, will abort this request", this);
            return 0;
        }
        try {
            if (this.isBeingArbitrated.get()) {
                this.condition.await();
            }
            long parentRes = getParentPool().requestMemoryInternal(bytes);
            if (parentRes > 0) {
                this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + parentRes);
                this.stats.setNumExpands(this.stats.getNumExpands() + 1);
            }
            return parentRes;
        } catch (InterruptedException e) {
            LOG.error("Failed to release self because ", e);
            Thread.currentThread().interrupt();
            return 0;
        }

    }
}
