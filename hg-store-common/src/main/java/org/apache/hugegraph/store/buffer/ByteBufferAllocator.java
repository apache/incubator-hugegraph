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

package org.apache.hugegraph.store.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class ByteBufferAllocator {
    // 每个bytebuffer大小
    final int capacity;
    // buffer 最大个数
    final int maxCount;
    // 空闲队列
    final BlockingQueue<ByteBuffer> freeQueue = new LinkedBlockingQueue<>();
    // 当前数量
    AtomicInteger totalCount;

    public ByteBufferAllocator(int cap, int count) {
        this.capacity = cap;
        this.maxCount = count;
        this.totalCount = new AtomicInteger(0);
    }

    public ByteBuffer get() throws InterruptedException {
        ByteBuffer buffer = null;
        while (buffer == null) {
            if (freeQueue.size() > 0) {
                buffer = freeQueue.poll();
            } else if (totalCount.get() < maxCount) {
                buffer = ByteBuffer.allocate(capacity);
                totalCount.incrementAndGet();
            } else {
                buffer = freeQueue.poll(1, TimeUnit.SECONDS);
            }
        }
        return buffer;
    }

    public void release(ByteBuffer buffer) {
        if (freeQueue.size() < maxCount) {
            buffer.clear();
            freeQueue.add(buffer);
        }
    }
}
