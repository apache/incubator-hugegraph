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

package org.apache.hugegraph.memory.allocator;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.util.Bytes;

public class OnHeapMemoryStrategy implements MemoryAllocator {

    private final MemoryManager memoryManager;

    public OnHeapMemoryStrategy(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public byte[] tryToAllocate(long size) {
        if (memoryManager.getCurrentOnHeapAllocatedMemory().get() +
            memoryManager.getCurrentOffHeapAllocatedMemory().get() + size <
            MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES) {
            int sizeOfByte = (int) (size / Bytes.BASE);
            memoryManager.getCurrentOnHeapAllocatedMemory().addAndGet(sizeOfByte);
            return new byte[sizeOfByte];
        }
        return null;
    }

    @Override
    public byte[] forceAllocate(long size) {
        int sizeOfByte = (int) (size / Bytes.BASE);
        memoryManager.getCurrentOnHeapAllocatedMemory().addAndGet(sizeOfByte);
        return new byte[sizeOfByte];
    }

    @Override
    public void returnMemoryToManager(long size) {
        memoryManager.getCurrentOnHeapAllocatedMemory().addAndGet(-size);
    }

    @Override
    public void releaseMemoryBlock(Object memoryBlock) {
        memoryBlock = null;
    }
}
