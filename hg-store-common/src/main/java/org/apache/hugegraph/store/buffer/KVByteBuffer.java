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

public class KVByteBuffer {
    ByteBuffer buffer;

    public KVByteBuffer(int capacity) {
        buffer = ByteBuffer.allocate(capacity);
    }

    public KVByteBuffer(byte[] buffer) {
        this.buffer = ByteBuffer.wrap(buffer);
    }

    public KVByteBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void clear() {
        this.buffer.clear();
    }

    public KVByteBuffer flip() {
        buffer.flip();
        return this;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public ByteBuffer copyBuffer() {
        byte[] buf = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, buf, 0, buffer.position());
        return ByteBuffer.wrap(buf);
    }

    public void put(byte data) {
        buffer.put(data);
    }

    public void put(byte[] data) {
        if (data != null) {
            buffer.putInt(data.length);
            buffer.put(data);
        }
    }

    public byte[] getBytes() {
        int len = buffer.getInt();
        byte[] data = new byte[len];
        buffer.get(data);
        return data;
    }

    public byte get() {
        return buffer.get();
    }

    public void putInt(int data) {
        buffer.putInt(data);
    }

    public int getInt() {
        return buffer.getInt();
    }

    public byte[] array() {
        return this.buffer.array();
    }

    public int position() {
        return this.buffer.position();
    }

    public final boolean hasRemaining() {
        return this.buffer.hasRemaining();
    }
}
