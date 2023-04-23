package com.baidu.hugegraph.store.buffer;


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
    // 当前数量
    AtomicInteger totalCount;
    // 空闲队列
    final BlockingQueue<ByteBuffer> freeQueue = new LinkedBlockingQueue<>();

    public ByteBufferAllocator(int cap, int count){
        this.capacity = cap;
        this.maxCount = count;
        this.totalCount = new AtomicInteger(0);
    }

    public ByteBuffer get() throws InterruptedException {
        ByteBuffer buffer = null;
        while (buffer == null) {
            if (freeQueue.size() > 0)
                buffer = freeQueue.poll();
            else if (totalCount.get() < maxCount) {
                buffer = ByteBuffer.allocate(capacity);
                totalCount.incrementAndGet();
            } else {
                buffer = freeQueue.poll(1, TimeUnit.SECONDS);
            }
        }
        return buffer;
    }

    public void release(ByteBuffer buffer) {
        if ( freeQueue.size() < maxCount) {
            buffer.clear();
            freeQueue.add(buffer);
        }
    }

}
