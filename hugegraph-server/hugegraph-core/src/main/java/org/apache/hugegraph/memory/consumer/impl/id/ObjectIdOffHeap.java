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

import java.util.Objects;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.memory.consumer.MemoryConsumer;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.FurySerializationUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class ObjectIdOffHeap extends IdGenerator.ObjectId implements MemoryConsumer {

    private final MemoryPool memoryPool;
    private ByteBuf objectOffHeap;

    public ObjectIdOffHeap(Object object, MemoryPool memoryPool) {
        super(object);
        this.memoryPool = memoryPool;
        serializeSelfToByteBuf();
        releaseOriginalOnHeapVars();
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        return new IdGenerator.ObjectId(FurySerializationUtils.FURY.deserialize(
                ByteBufUtil.getBytes(this.objectOffHeap)));
    }

    @Override
    public void serializeSelfToByteBuf() {
        byte[] bytes = FurySerializationUtils.FURY.serialize(object);
        this.objectOffHeap = (ByteBuf) memoryPool.requireMemory(bytes.length);
        this.objectOffHeap.markReaderIndex();
        this.objectOffHeap.writeBytes(bytes);
        //try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        //     ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
        //    // serialize attributes to outputStream
        //    outputStream.writeObject(this.object);
        //    objectOffHeap = (ByteBuf) memoryPool.requireMemory(byteArrayOutputStream.size());
        //    objectOffHeap.writeBytes(byteArrayOutputStream.toByteArray());
        //} catch (IOException e) {
        //    LOG.error("Unexpected error occurs when serializing ObjectId.", e);
        //    throw new SerializationRuntimeException(e);
        //}
    }

    @Override
    public void releaseOriginalOnHeapVars() {
        this.object = null;
    }

    @Override
    public MemoryPool getOperatorMemoryPool() {
        return memoryPool;
    }

    @Override
    public Object asObject() {
        return FurySerializationUtils.FURY.deserialize(ByteBufUtil.getBytes(objectOffHeap));
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectOffHeap);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ObjectIdOffHeap)) {
            return false;
        }
        return this.objectOffHeap.equals(((ObjectIdOffHeap) other).objectOffHeap);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}



