/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package common;

import java.util.concurrent.CountDownLatch;

import org.apache.hugegraph.store.buffer.ByteBufferAllocator;
import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ByteBufferAllocatorTest extends BaseCommonTest {
    @Test
    public void getAndReleaseTest() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);

        ByteBufferAllocator allocator = new ByteBufferAllocator(1, 2);

        new Thread(() -> {
            try {
                var buffer1 = allocator.get();
                var buffer2 = allocator.get();
                Thread.sleep(2000);
                Assert.assertEquals(buffer1.limit(), 1);
                allocator.release(buffer1);
                allocator.release(buffer2);
                latch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(1000);
                var buffer1 = allocator.get();
                var buffer2 = allocator.get();
                Assert.assertEquals(buffer1.limit(), 1);
                allocator.release(buffer1);
                allocator.release(buffer2);
                latch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();
    }
}
