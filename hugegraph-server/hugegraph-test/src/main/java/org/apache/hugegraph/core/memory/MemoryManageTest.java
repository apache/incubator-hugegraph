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

package org.apache.hugegraph.core.memory;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.pool.impl.TaskMemoryPool;
import org.apache.hugegraph.memory.util.RoundUtil;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;

/**
 * This test will construct a query context with 2 query for testing memory management
 * <p>
 * query1
 * |
 * --> task1
 * |
 * ----> operator1
 * |
 * ----> operator2
 * |
 * --> task2
 * |
 * ----> operator1
 * <p>
 * query2
 * |
 * --> task1
 * |
 * ----> operator1
 */
public class MemoryManageTest {

    protected static MemoryManager memoryManager;
    protected static MemoryPool query1MemoryPool;
    protected static MemoryPool query1Task1MemoryPool;
    protected static String QUERY1_TASK1_THREAD_NAME = "main";
    protected static MemoryPool query1Task2MemoryPool;
    protected static String QUERY1_TASK2_THREAD_NAME = "QUERY1-THREAD-2";
    protected static MemoryPool query1Task1Operator1MemoryPool;
    protected static MemoryPool query1Task1Operator2MemoryPool;
    protected static MemoryPool query1Task2Operator1MemoryPool;

    protected static MemoryPool query2MemoryPool;
    protected static MemoryPool query2Task1MemoryPool;
    protected static String QUERY2_TASK1_THREAD_NAME = "QUERY2-THREAD-1";
    protected static MemoryPool query2Task1Operator1MemoryPool;

    @Before
    public void setUp() {
        memoryManager = MemoryManager.getInstance();
        MemoryManager.setMemoryMode(MemoryManager.MemoryMode.ENABLE_OFF_HEAP_MANAGEMENT);
        MemoryManager.setMaxMemoryCapacityInBytes(Bytes.GB);
        MemoryManager.setMaxMemoryCapacityForOneQuery(Bytes.MB * 100);
        RoundUtil.setAlignment(8);
        query1MemoryPool = memoryManager.addQueryMemoryPool();
        query2MemoryPool = memoryManager.addQueryMemoryPool();

        query1Task1MemoryPool = query1MemoryPool.addChildPool("Task1");
        memoryManager.bindCorrespondingTaskMemoryPool(QUERY1_TASK1_THREAD_NAME,
                                                      (TaskMemoryPool) query1Task1MemoryPool);
        query1Task1Operator1MemoryPool = query1Task1MemoryPool.addChildPool("Operator1");
        query1Task1Operator2MemoryPool = query1Task1MemoryPool.addChildPool("Operator2");

        query1Task2MemoryPool = query1MemoryPool.addChildPool("Task2");
        memoryManager.bindCorrespondingTaskMemoryPool(QUERY1_TASK2_THREAD_NAME,
                                                      (TaskMemoryPool) query1Task2MemoryPool);
        query1Task2Operator1MemoryPool = query1Task2MemoryPool.addChildPool("Operator1");

        query2Task1MemoryPool = query2MemoryPool.addChildPool("Task1");
        memoryManager.bindCorrespondingTaskMemoryPool(QUERY2_TASK1_THREAD_NAME,
                                                      (TaskMemoryPool) query2Task1MemoryPool);
        query2Task1Operator1MemoryPool = query2Task1MemoryPool.addChildPool("Operator1");
    }

    @After
    public void after() {
        memoryManager.gcQueryMemoryPool(query1MemoryPool);
        memoryManager.gcQueryMemoryPool(query2MemoryPool);
        memoryManager.getCurrentAvailableMemoryInBytes()
                     .set(MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES);
        Assert.assertEquals(0, memoryManager.getCurrentQueryMemoryPools().size());
        Assert.assertEquals(0, query1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query2MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query1Task1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query2Task1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query1Task1Operator2MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query1Task1Operator1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query2Task1Operator1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
    }

    @Test
    public void testCurrentWorkingMemoryPool() {
        TaskMemoryPool taskMemoryPool =
                (TaskMemoryPool) memoryManager.getCorrespondingTaskMemoryPool(
                        Thread.currentThread()
                              .getName());
        Assert.assertNotNull(taskMemoryPool);
        Assert.assertNotNull(taskMemoryPool.getCurrentWorkingOperatorMemoryPool());
    }

    @Test
    public void testMemoryPoolStructure() {
        Assert.assertEquals(2, memoryManager.getCurrentQueryMemoryPools().size());
        Assert.assertEquals(2, query1MemoryPool.getChildrenCount());
        Assert.assertEquals(2, query1Task1MemoryPool.getChildrenCount());
        Assert.assertEquals(1, query1Task2MemoryPool.getChildrenCount());
        Assert.assertEquals(0, query1Task1Operator1MemoryPool.getChildrenCount());

        Assert.assertEquals(1, query2MemoryPool.getChildrenCount());
        Assert.assertEquals(1, query2Task1MemoryPool.getChildrenCount());
        Assert.assertEquals(0, query2Task1Operator1MemoryPool.getChildrenCount());
    }

    @Test
    public void testRequiringMemory() {
        long requireBytes = Bytes.KB;
        ByteBuf memoryBlock = (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes
                , query1Task1Operator1MemoryPool);
        Assert.assertNotNull(memoryBlock);
        Assert.assertEquals(requireBytes, memoryBlock.capacity());
        Assert.assertEquals(requireBytes,
                            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
        Assert.assertEquals(
                MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES - query1MemoryPool.getAllocatedBytes(),
                memoryManager.getCurrentAvailableMemoryInBytes().get());
        // will use reserved memory, not requiring memory through manager
        ByteBuf memoryBlock2 =
                (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes,
                                                                       query1Task1Operator1MemoryPool);
        Assert.assertNotNull(memoryBlock2);
        Assert.assertEquals(requireBytes, memoryBlock2.capacity());
        Assert.assertEquals(requireBytes * 2,
                            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
        Assert.assertEquals(
                MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES - query1MemoryPool.getAllocatedBytes(),
                memoryManager.getCurrentAvailableMemoryInBytes().get());
    }

    @Test
    public void testOOM() {
        long requireBytes = Bytes.GB * 2;
        ByteBuf memoryBlock = (ByteBuf) query2Task1Operator1MemoryPool.requireMemory(requireBytes
                , query2Task1Operator1MemoryPool);
        Assert.assertNull(memoryBlock);
    }

    @Test
    public void testReleaseMemoryWithTask() {
        long requireBytes = Bytes.KB;
        ByteBuf memoryBlock = (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes
                , query1Task1Operator1MemoryPool);
        Assert.assertNotNull(memoryBlock);
        Assert.assertEquals(requireBytes, memoryBlock.capacity());
        memoryManager.getCorrespondingTaskMemoryPool(QUERY1_TASK1_THREAD_NAME)
                     .releaseSelf("Test release by hand", false);
        Assert.assertNull(memoryManager.getCorrespondingTaskMemoryPool(QUERY1_TASK1_THREAD_NAME));
        Assert.assertEquals(1, query1MemoryPool.getChildrenCount());
        Assert.assertEquals(0, query1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query1MemoryPool.getUsedBytes());
        Assert.assertEquals(0, query1Task1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query1Task1MemoryPool.getUsedBytes());
        Assert.assertEquals(0, memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
        Assert.assertEquals(MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES,
                            memoryManager.getCurrentAvailableMemoryInBytes().get());
    }

    @Test
    public void testReleaseMemoryWithQuery() {
        long requireBytes = Bytes.KB;
        ByteBuf memoryBlock = (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes
                , query1Task1Operator1MemoryPool);
        Assert.assertNotNull(memoryBlock);
        Assert.assertEquals(requireBytes, memoryBlock.capacity());
        query1MemoryPool.releaseSelf("Test release by hand", true);
        Assert.assertEquals(0, query1MemoryPool.getChildrenCount());
        Assert.assertEquals(0, query1MemoryPool.getAllocatedBytes());
        Assert.assertEquals(0, query1MemoryPool.getUsedBytes());
        Assert.assertEquals(0, memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
        Assert.assertEquals(MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES,
                            memoryManager.getCurrentAvailableMemoryInBytes().get());
    }

    @Test
    public void testExpandCapacity() {
        long requireBytes = Bytes.KB;
        ByteBuf memoryBlock = (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes
                , query1Task1Operator1MemoryPool);
        Assert.assertNotNull(memoryBlock);
        Assert.assertEquals(requireBytes, memoryBlock.capacity());
        long maxCapacity = Bytes.KB * 100;
        query2MemoryPool.setMaxCapacityBytes(maxCapacity);
        long requireBytes2 = maxCapacity * 2;
        ByteBuf memoryBlock2 =
                (ByteBuf) query2Task1Operator1MemoryPool.requireMemory(requireBytes2,
                                                                       query2Task1Operator1MemoryPool);
        Assert.assertNotNull(memoryBlock2);
        Assert.assertEquals(requireBytes2, memoryBlock2.capacity());
        Assert.assertEquals(requireBytes2 + requireBytes,
                            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
        Assert.assertEquals(
                MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES - query1MemoryPool.getAllocatedBytes() -
                query2MemoryPool.getAllocatedBytes(),
                memoryManager.getCurrentAvailableMemoryInBytes().get());

    }

    @Test
    public void testLocalArbitration() {
        long totalMemory = 2 * Bytes.MB + Bytes.KB;
        memoryManager.getCurrentAvailableMemoryInBytes().set(totalMemory);
        long requireBytes = Bytes.KB;
        // will allocate 2MB
        ByteBuf memoryBlock = (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes
                , query1Task1Operator1MemoryPool);
        long requireBytes2 = Bytes.MB;
        ByteBuf memoryBlock2 =
                (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes2,
                                                                       query1Task1Operator1MemoryPool);
        Assert.assertEquals(Bytes.MB * 2, query1MemoryPool.getAllocatedBytes());
        // query1 remaining 1023KB
        Assert.assertEquals(Bytes.MB * 2 - requireBytes2 - requireBytes,
                            query1MemoryPool.getFreeBytes());
        // totally remaining 1KB
        Assert.assertEquals(totalMemory - 2 * Bytes.MB,
                            memoryManager.getCurrentAvailableMemoryInBytes().get());
        // will try to allocate 1MB and trigger arbitration, which will fail and result in OOM
        ByteBuf memoryBlock3 =
                (ByteBuf) query2Task1Operator1MemoryPool.requireMemory(requireBytes,
                                                                       query2Task1Operator1MemoryPool);
        Assert.assertNull(memoryBlock3);
        Assert.assertEquals(0, query2MemoryPool.getAllocatedBytes());
    }

    @Test
    public void testGlobalArbitration() {
        long totalMemory = 20 * Bytes.MB + Bytes.KB;
        memoryManager.getCurrentAvailableMemoryInBytes().set(totalMemory);
        long requireBytes = Bytes.MB * 17;
        // will allocate 20MB
        ByteBuf memoryBlock = (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes
                , query1Task1Operator1MemoryPool);
        Assert.assertEquals(Bytes.MB * 20, query1MemoryPool.getAllocatedBytes());
        // query1 remaining 3MB
        Assert.assertEquals(Bytes.MB * 3,
                            query1MemoryPool.getFreeBytes());
        // totally remaining 1KB
        Assert.assertEquals(Bytes.KB, memoryManager.getCurrentAvailableMemoryInBytes().get());
        // will try to allocate 1MB and trigger arbitration, which will success
        long requireBytes2 = Bytes.KB;
        ByteBuf memoryBlock2 =
                (ByteBuf) query2Task1Operator1MemoryPool.requireMemory(requireBytes2,
                                                                       query2Task1Operator1MemoryPool);
        Assert.assertNotNull(memoryBlock2);
        Assert.assertEquals(Bytes.MB, query2MemoryPool.getAllocatedBytes());
        Assert.assertEquals(Bytes.KB, memoryBlock2.capacity());
        // totally still remain 1KB
        Assert.assertEquals(Bytes.KB, memoryManager.getCurrentAvailableMemoryInBytes().get());
    }
}
