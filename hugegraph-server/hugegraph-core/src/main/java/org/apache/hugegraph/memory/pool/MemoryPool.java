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

    long tryToReclaimLocalMemory(long neededBytes);

    Object requireMemory(long bytes);

    long requestMemoryInternal(long bytes) throws OutOfMemoryException;

    Object tryToAcquireMemoryInternal(long bytes);

    /**
     * Release all self's resources. Called by user or called automatically by itself when OOM.
     *
     * @param reason:              release reason, for logging.
     * @param isTriggeredInternal: if true, it is called automatically. if false, called by user.
     */
    void releaseSelf(String reason, boolean isTriggeredInternal);

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
