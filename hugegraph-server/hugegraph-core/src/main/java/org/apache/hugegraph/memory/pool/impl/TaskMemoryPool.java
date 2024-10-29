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
import org.apache.hugegraph.memory.util.OutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskMemoryPool extends AbstractMemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMemoryPool.class);
    private static final String OPERATOR_MEMORY_POOL_NAME_PREFIX = "OperatorMemoryPool";
    // One thread corresponds to one task memory pool. Since the execution flow within a single
    // thread is serial, there is only one working Operator pool in the thread at each time.
    // This variable helps the execution flow obtain the memory management context.
    private MemoryPool CURRENT_WORKING_OPERATOR_MEMORY_POOL = null;

    public TaskMemoryPool(MemoryPool parent, String poolName, MemoryManager memoryManager) {
        super(parent, poolName, MemoryPoolStats.MemoryPoolType.TASK, memoryManager);
    }

    public MemoryPool getCurrentWorkingOperatorMemoryPool() {
        return CURRENT_WORKING_OPERATOR_MEMORY_POOL;
    }

    @Override
    public long tryToReclaimLocalMemory(long neededBytes, MemoryPool requestingPool) {
        if (isClosed) {
            LOG.warn("[{}] is already closed, will abort this reclaim", this);
            return 0;
        }
        if (this.findRootQueryPool().equals(requestingPool.findRootQueryPool())) {
            return super.tryToReclaimLocalMemoryWithoutLock(neededBytes, requestingPool);
        }
        return super.tryToReclaimLocalMemory(neededBytes, requestingPool);
    }

    @Override
    public void releaseSelf(String reason, boolean isTriggeredByOOM) {
        super.releaseSelf(reason, isTriggeredByOOM);
        this.memoryManager.removeCorrespondingTaskMemoryPool(Thread.currentThread().getName());
    }

    @Override
    public MemoryPool addChildPool(String name) {
        int count = this.children.size();
        String poolName =
                OPERATOR_MEMORY_POOL_NAME_PREFIX + DELIMINATOR + name + DELIMINATOR + count +
                DELIMINATOR + System.currentTimeMillis();
        MemoryPool operatorPool =
                new OperatorMemoryPool(this, poolName,
                                       new NettyMemoryAllocator(this.memoryManager),
                                       this.memoryManager);
        this.children.add(operatorPool);
        CURRENT_WORKING_OPERATOR_MEMORY_POOL = operatorPool;
        LOG.info("TaskPool-{} added operator memory pool {}", this, operatorPool);
        return operatorPool;
    }

    @Override
    public Object tryToAcquireMemoryInternal(long bytes) {
        getParentPool().tryToAcquireMemoryInternal(bytes);
        return super.tryToAcquireMemoryInternal(bytes);
    }

    @Override
    public long requestMemoryInternal(long bytes, MemoryPool requestingPool) throws
                                                                             OutOfMemoryException {
        if (this.isClosed) {
            LOG.warn("[{}] is already closed, will abort this request", this);
            return 0;
        }
        memoryActionLock.lock();
        try {
            if (this.isBeingArbitrated.get()) {
                this.condition.await();
            }
            long parentRes = getParentPool().requestMemoryInternal(bytes, requestingPool);
            if (parentRes > 0) {
                this.stats.setAllocatedBytes(this.stats.getAllocatedBytes() + parentRes);
                this.stats.setNumExpands(this.stats.getNumExpands() + 1);
            }
            return parentRes;
        } catch (InterruptedException e) {
            LOG.error("Failed to release self because ", e);
            Thread.currentThread().interrupt();
            return 0;
        } finally {
            memoryActionLock.unlock();
        }
    }
}
