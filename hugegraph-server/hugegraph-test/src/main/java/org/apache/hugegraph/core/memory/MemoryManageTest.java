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
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Bytes;
import org.junit.BeforeClass;
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
    protected static String QUERY1_TASK1_THREAD_NAME = "QUERY1-THREAD-1";
    protected static MemoryPool query1Task2MemoryPool;
    protected static String QUERY1_TASK2_THREAD_NAME = "QUERY1-THREAD-2";
    protected static MemoryPool query1Task1Operator1MemoryPool;
    protected static MemoryPool query1Task1Operator2MemoryPool;
    protected static MemoryPool query1Task2Operator1MemoryPool;

    protected static MemoryPool query2MemoryPool;
    protected static MemoryPool query2Task1MemoryPool;
    protected static String QUERY2_TASK1_THREAD_NAME = "QUERY2-THREAD-1";
    protected static MemoryPool query2Task1Operator1MemoryPool;

    @BeforeClass
    public static void setUp() {
        memoryManager = MemoryManager.getInstance();
        query1MemoryPool = memoryManager.addQueryMemoryPool();
        query2MemoryPool = memoryManager.addQueryMemoryPool();

        query1Task1MemoryPool = query1MemoryPool.addChildPool();
        memoryManager.bindCorrespondingTaskMemoryPool(QUERY1_TASK1_THREAD_NAME,
                                                      query1Task1MemoryPool);
        query1Task1Operator1MemoryPool = query1Task1MemoryPool.addChildPool();
        query1Task1Operator2MemoryPool = query1Task1MemoryPool.addChildPool();

        query1Task2MemoryPool = query1MemoryPool.addChildPool();
        memoryManager.bindCorrespondingTaskMemoryPool(QUERY1_TASK2_THREAD_NAME,
                                                      query1Task2MemoryPool);
        query1Task2Operator1MemoryPool = query1Task2MemoryPool.addChildPool();

        query2Task1MemoryPool = query2MemoryPool.addChildPool();
        memoryManager.bindCorrespondingTaskMemoryPool(QUERY2_TASK1_THREAD_NAME,
                                                      query2Task1MemoryPool);
        query2Task1Operator1MemoryPool = query2Task1MemoryPool.addChildPool();
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
        ByteBuf memoryBlock = (ByteBuf) query1Task1Operator1MemoryPool.requireMemory(requireBytes);
        Assert.assertNotNull(memoryBlock);
        Assert.assertEquals(requireBytes, memoryBlock.capacity());
    }

    @Test
    public void testOOM() {
        long requireBytes = Bytes.GB * 2;
        ByteBuf memoryBlock = (ByteBuf) query2Task1Operator1MemoryPool.requireMemory(requireBytes);
        Assert.assertNull(memoryBlock);
    }
}
