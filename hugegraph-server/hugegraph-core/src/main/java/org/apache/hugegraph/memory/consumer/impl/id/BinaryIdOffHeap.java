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

package org.apache.hugegraph.memory.consumer.impl.id;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.memory.consumer.MemoryConsumer;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class BinaryIdOffHeap extends BinaryBackendEntry.BinaryId implements MemoryConsumer {

    private final MemoryPool memoryPool;
    private final MemoryConsumer originId;
    private ByteBuf bytesOffHeap;

    public BinaryIdOffHeap(byte[] bytes, Id id, MemoryPool memoryPool, MemoryConsumer originId) {
        super(bytes, id);
        this.memoryPool = memoryPool;
        this.originId = originId;
        serializeSelfToByteBuf();
        releaseOriginalOnHeapVars();
    }

    @Override
    public void serializeSelfToByteBuf() {
        this.bytesOffHeap = (ByteBuf) memoryPool.requireMemory(bytes.length);
        this.bytesOffHeap.markReaderIndex();
        this.bytesOffHeap.writeBytes(bytes);
    }

    @Override
    public BinaryBackendEntry.BinaryId zeroCopyReadFromByteBuf() {
        return new BinaryBackendEntry.BinaryId(ByteBufUtil.getBytes(bytesOffHeap),
                                               (Id) originId.zeroCopyReadFromByteBuf());
    }

    @Override
    public MemoryPool getOperatorMemoryPool() {
        return this.memoryPool;
    }

    @Override
    public void releaseOriginalOnHeapVars() {
        this.bytes = null;
        this.id = null;
    }

    @Override
    public Object asObject() {
        return bytesOffHeap.nioBuffer();
    }

    @Override
    public String toString() {
        return "0x" + Bytes.toHex(asBytes());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BinaryIdOffHeap)) {
            return false;
        }
        return bytesOffHeap.equals(((BinaryIdOffHeap) other).bytesOffHeap);
    }

    @Override
    public int hashCode() {
        return bytesOffHeap.hashCode();
    }

    @Override
    public int length() {
        return bytesOffHeap.readableBytes();
    }

    @Override
    public byte[] asBytes(int offset) {
        E.checkArgument(offset < this.bytesOffHeap.readableBytes(),
                        "Invalid offset %s, must be < length %s",
                        offset, this.bytesOffHeap.readableBytes());
        try {
            // zero-copy read
            byte[] tmpBytes = new byte[offset];
            this.bytesOffHeap.readBytes(tmpBytes);
            return tmpBytes;
        } finally {
            this.bytesOffHeap.resetReaderIndex();
        }
    }

    @Override
    public byte[] asBytes() {
        try {
            // zero-copy read
            byte[] tmpBytes = new byte[bytesOffHeap.readableBytes()];
            this.bytesOffHeap.readBytes(tmpBytes);
            return tmpBytes;
        } finally {
            this.bytesOffHeap.resetReaderIndex();
        }
    }

    @Override
    public int compareTo(Id other) {
        return bytesOffHeap.compareTo(((BinaryIdOffHeap) other).bytesOffHeap);
    }

    @Override
    public Id origin() {
        return (Id) originId.zeroCopyReadFromByteBuf();
    }
}
