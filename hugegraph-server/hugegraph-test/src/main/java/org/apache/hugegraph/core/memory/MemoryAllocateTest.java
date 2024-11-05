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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.allocator.MemoryAllocator;
import org.apache.hugegraph.memory.allocator.NettyMemoryAllocator;
import org.apache.hugegraph.memory.allocator.OnHeapMemoryAllocator;
import org.apache.hugegraph.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;

public class MemoryAllocateTest {

    private static MemoryAllocator nettyAllocator;
    private static MemoryAllocator onHeapAllocator;
    private static MemoryManager memoryManager;

    @Before
    public void setup() {
        MemoryManager.getInstance().getCurrentOffHeapAllocatedMemoryInBytes().set(0);
        MemoryManager.getInstance().getCurrentOnHeapAllocatedMemoryInBytes().set(0);
    }

    @BeforeClass
    public static void beforeClass() {
        nettyAllocator = new NettyMemoryAllocator(MemoryManager.getInstance());
        onHeapAllocator = new OnHeapMemoryAllocator(MemoryManager.getInstance());
        memoryManager = MemoryManager.getInstance();
    }

    @Test
    public void testNettyAllocate() {
        ByteBuf memoryBlock1 = (ByteBuf) nettyAllocator.tryToAllocate(Bytes.KB);
        ByteBuf memoryBlock2 = (ByteBuf) nettyAllocator.tryToAllocate(Bytes.MB);
        Assert.assertNotNull(memoryBlock1);
        Assert.assertEquals(Bytes.KB, memoryBlock1.capacity());
        Assert.assertNotNull(memoryBlock2);
        Assert.assertEquals(Bytes.MB, memoryBlock2.capacity());
        Assert.assertEquals(Bytes.KB + Bytes.MB,
                            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
        ByteBuf memoryBlock3 = (ByteBuf) nettyAllocator.tryToAllocate(Bytes.GB);
        Assert.assertNull(memoryBlock3);
        memoryBlock3 = (ByteBuf) nettyAllocator.forceAllocate(Bytes.GB);
        Assert.assertNotNull(memoryBlock3);
        Assert.assertEquals(Bytes.GB, memoryBlock3.capacity());
    }

    @Test
    public void testNettyDeallocate() {
        ByteBuf buf = (ByteBuf) nettyAllocator.tryToAllocate(Bytes.KB);
        Assert.assertNotNull(buf);
        Assert.assertTrue(buf.isWritable());
        Assert.assertEquals(buf.capacity(),
                            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
        Assert.assertEquals(1, ReferenceCountUtil.refCnt(buf));
        nettyAllocator.releaseMemoryBlock(buf);
        Assert.assertThrows(IllegalReferenceCountException.class, buf::memoryAddress);
        Assert.assertEquals(0, ReferenceCountUtil.refCnt(buf));
        nettyAllocator.returnMemoryToManager(buf.capacity());
        Assert.assertEquals(0,
                            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get());
    }

    @Test
    public void testOnHeapAllocate() {
        AtomicReference<byte[]> memoryBlock1 =
                (AtomicReference<byte[]>) onHeapAllocator.tryToAllocate(Bytes.KB);
        AtomicReference<byte[]> memoryBlock2 =
                (AtomicReference<byte[]>) onHeapAllocator.tryToAllocate(Bytes.MB);
        Assert.assertNotNull(memoryBlock1);
        Assert.assertEquals(Bytes.KB, memoryBlock1.get().length);
        Assert.assertNotNull(memoryBlock2);
        Assert.assertEquals(Bytes.MB, memoryBlock2.get().length);
        Assert.assertEquals(Bytes.KB + Bytes.MB,
                            memoryManager.getCurrentOnHeapAllocatedMemoryInBytes().get());
        AtomicReference<byte[]> memoryBlock3 =
                (AtomicReference<byte[]>) onHeapAllocator.tryToAllocate(Bytes.GB);
        Assert.assertNull(memoryBlock3);
        memoryBlock3 = (AtomicReference<byte[]>) onHeapAllocator.forceAllocate(Bytes.GB);
        Assert.assertNotNull(memoryBlock3);
        Assert.assertEquals(Bytes.GB, memoryBlock3.get().length);
    }

    @Test
    public void testOnHeapDeallocate() {
        AtomicReference<byte[]> buf =
                (AtomicReference<byte[]>) onHeapAllocator.tryToAllocate(Bytes.KB);
        Assert.assertNotNull(buf);
        Assert.assertEquals(buf.get().length,
                            memoryManager.getCurrentOnHeapAllocatedMemoryInBytes().get());
        onHeapAllocator.returnMemoryToManager(buf.get().length);
        Assert.assertEquals(0,
                            memoryManager.getCurrentOnHeapAllocatedMemoryInBytes().get());
        onHeapAllocator.releaseMemoryBlock(buf);
        Assert.assertNull(buf.get());
    }
}
