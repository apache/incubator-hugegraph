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

import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.impl.MemoryPoolStats;
import org.apache.hugegraph.memory.util.OutOfMemoryException;
import org.jetbrains.annotations.TestOnly;

public interface MemoryPool {

    MemoryPoolStats getSnapShot();

    /**
     * Try to reclaim self's free memory, triggered by an arbitration.
     *
     * @param neededBytes    reclaim goal, which means this reclaim will try to collect neededBytes.
     * @param requestingPool the memoryPool that requests this arbitration.
     * @return reclaimed memory in bytes.
     */
    long tryToReclaimLocalMemory(long neededBytes, MemoryPool requestingPool);

    /**
     * Require to be allocated with a memory block. This method contains two steps:
     * <p>
     * 1. Request manager for a logic allocation. Throw OOM exception if memory exhausted.
     * 2. Request allocator for a real allocation. Return real memory block.
     *
     * @param bytes          needed bytes.
     * @param requestingPool the memoryPool that requests this allocation.
     * @return memory block. ByteBuf if using off-heap, byte[] if using on-heap.
     */
    Object requireMemory(long bytes, MemoryPool requestingPool);

    /**
     * Request MemoryManager for a logic allocation. This method is used internal in {@code
     * requireMemory}.
     *
     * @param bytes          needed bytes.
     * @param requestingPool the memoryPool that requests this allocation.
     * @return the memory size that upper manager can provide.
     * @throws OutOfMemoryException if there isn't enough memory in manager.
     */
    long requestMemoryInternal(long bytes, MemoryPool requestingPool) throws OutOfMemoryException;

    /**
     * Request allocator for a real allocation. This method is used internal in {@code
     * requireMemory}.
     *
     * @param bytes needed bytes.
     * @return memory block. ByteBuf if using off-heap, byte[] if using on-heap.
     */
    Object tryToAcquireMemoryInternal(long bytes);

    /**
     * Release all self's resources. Called by user or called automatically by itself when OOM.
     *
     * @param reason           release reason, for logging.
     * @param isTriggeredByOOM if true, it is called when OOM. if false, called by user.
     */
    void releaseSelf(String reason, boolean isTriggeredByOOM);

    /**
     * Called by `releaseSelf` to release children's resource.
     *
     * @param child:               child pool
     * @param force:               if false, called to gc self from father
     * @param isTriggeredInternal: passed from upper caller `releaseSelf`
     */
    void gcChildPool(MemoryPool child, boolean force, boolean isTriggeredInternal);

    long getAllocatedBytes();

    long getUsedBytes();

    long getFreeBytes();

    long getMaxCapacityBytes();

    String getName();

    MemoryPool getParentPool();

    MemoryPool findRootQueryPool();

    MemoryPool addChildPool(String name);

    void bindMemoryConsumer(OffHeapObject offHeapObject);

    void setMaxCapacityBytes(long maxCapacityBytes);

    @TestOnly
    int getChildrenCount();
}
