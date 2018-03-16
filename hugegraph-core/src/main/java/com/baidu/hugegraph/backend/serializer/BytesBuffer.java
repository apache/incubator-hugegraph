/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.backend.serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry.BinaryId;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

/**
 * class BytesBuffer is a util for read/write binary
 */
public final class BytesBuffer {

    public static final int BYTE_LEN = Byte.BYTES;
    public static final int SHORT_LEN = Short.BYTES;
    public static final int INT_LEN = Integer.BYTES;
    public static final int LONG_LEN = Long.BYTES;
    public static final int CHAR_LEN = Character.BYTES;
    public static final int FLOAT_LEN = Float.BYTES;
    public static final int DOUBLE_LEN = Double.BYTES;

    public static final int UINT8_MAX = ((byte) -1) & 0xff;
    public static final int UINT16_MAX = ((short) -1) & 0xffff;
    public static final long UINT32_MAX = (-1) & 0xffffffffL;

    public static final int ID_MAX_LEN = UINT8_MAX & 0x7f + 1; // 128
    public static final int BIG_ID_MAX_LEN = UINT16_MAX & 0x7fff + 1; // 32768

    public static final int DEFAULT_CAPACITY = 64;
    public static final int MAX_BUFFER_CAPACITY = 128 * 1024 * 1024; // 128M

    private ByteBuffer buffer;

    public BytesBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public BytesBuffer(int capacity) {
        E.checkArgument(capacity <= MAX_BUFFER_CAPACITY,
                        "Capacity exceeds max buffer capacity: %s",
                        MAX_BUFFER_CAPACITY);
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public BytesBuffer(ByteBuffer buffer) {
        E.checkNotNull(buffer, "buffer");
        this.buffer = buffer;
    }

    public static BytesBuffer allocate(int capacity) {
        return new BytesBuffer(capacity);
    }

    public static BytesBuffer wrap(byte[] array) {
        return new BytesBuffer(ByteBuffer.wrap(array));
    }

    public static BytesBuffer wrap(byte[] array, int offset, int length) {
        return new BytesBuffer(ByteBuffer.wrap(array, offset, length));
    }

    public ByteBuffer asByteBuffer() {
        return this.buffer;
    }

    public byte[] array() {
        return this.buffer.array();
    }

    public byte[] bytes() {
        byte[] bytes = this.buffer.array();
        if (this.buffer.position() == bytes.length) {
            return bytes;
        } else {
            return Arrays.copyOf(bytes, this.buffer.position());
        }
    }

    private void require(int size) {
        // Does need to resize?
        if (this.buffer.capacity() - this.buffer.position() >= size) {
            return;
        }

        // Extra capacity as buffer
        int newcapacity = size + this.buffer.capacity() + DEFAULT_CAPACITY;
        E.checkArgument(newcapacity <= MAX_BUFFER_CAPACITY,
                        "Capacity exceeds max buffer capacity: %s",
                        MAX_BUFFER_CAPACITY);
        ByteBuffer newBuffer = ByteBuffer.allocate(newcapacity);
        this.buffer.flip();
        newBuffer.put(this.buffer);
        this.buffer = newBuffer;
    }

    public BytesBuffer write(byte val) {
        require(BYTE_LEN);
        this.buffer.put(val);
        return this;
    }

    public BytesBuffer write(byte[] val) {
        require(BYTE_LEN * val.length);
        this.buffer.put(val);
        return this;
    }

    public BytesBuffer writeBoolean(boolean val) {
        return this.write((byte) (val ? 1 : 0));
    }

    public BytesBuffer writeChar(char val) {
        require(CHAR_LEN);
        this.buffer.putChar(val);
        return this;
    }

    public BytesBuffer writeShort(short val) {
        require(SHORT_LEN);
        this.buffer.putShort(val);
        return this;
    }

    public BytesBuffer writeInt(int val) {
        require(INT_LEN);
        this.buffer.putInt(val);
        return this;
    }

    public BytesBuffer writeLong(long val) {
        require(LONG_LEN);
        this.buffer.putLong(val);
        return this;
    }

    public BytesBuffer writeFloat(float val) {
        require(FLOAT_LEN);
        this.buffer.putFloat(val);
        return this;
    }

    public BytesBuffer writeDouble(double val) {
        require(DOUBLE_LEN);
        this.buffer.putDouble(val);
        return this;
    }

    public BytesBuffer writeBytes(byte[] bytes) {
        E.checkArgument(bytes.length <= UINT16_MAX,
                        "The max length of bytes is %s, got %s",
                        UINT16_MAX, bytes.length);
        require(SHORT_LEN + bytes.length);
        this.writeUInt16(bytes.length);
        this.write(bytes);
        return this;
    }

    public BytesBuffer writeString(String val) {
        byte[] bytes = StringEncoding.encode(val);
        this.writeBytes(bytes);
        return this;
    }

    public byte read() {
        return this.buffer.get();
    }

    public byte[] read(int length) {
        byte[] bytes = new byte[length];
        this.buffer.get(bytes);
        return bytes;
    }

    public boolean readBoolean() {
        return this.buffer.get() == 0 ? false : true;
    }

    public char readChar() {
        return this.buffer.getChar();
    }

    public short readShort() {
        return this.buffer.getShort();
    }

    public int readInt() {
        return this.buffer.getInt();
    }

    public long readLong() {
        return this.buffer.getLong();
    }

    public float readFloat() {
        return this.buffer.getFloat();
    }

    public double readDouble() {
        return this.buffer.getDouble();
    }

    public byte[] readBytes() {
        int length = this.readUInt16();
        byte[] bytes = this.read(length);
        return bytes;
    }

    public String readString() {
        return StringEncoding.decode(this.readBytes());
    }

    public BytesBuffer writeUInt8(int val) {
        assert val <= UINT8_MAX;
        this.write((byte) val);
        return this;
    }

    public int readUInt8() {
        return this.read() & 0x000000ff;
    }

    public BytesBuffer writeUInt16(int val) {
        assert val <= UINT16_MAX;
        this.writeShort((short) val);
        return this;
    }

    public int readUInt16() {
        return this.readShort() & 0x0000ffff;
    }

    public BytesBuffer writeUInt32(long val) {
        assert val <= UINT32_MAX;
        this.writeInt((int) val);
        return this;
    }

    public long readUInt32() {
        return this.readInt() & 0xffffffff;
    }

    public BytesBuffer writeStringToRemaining(String value) {
        byte[] bytes = StringEncoding.encode(value);
        this.write(bytes);
        return this;
    }

    public String readStringFromRemaining() {
        byte[] bytes = new byte[this.buffer.remaining()];
        this.buffer.get(bytes);
        return StringEncoding.decode(bytes);
    }

    public BytesBuffer writeId(Id id) {
        return this.writeId(id, false);
    }

    public BytesBuffer writeId(Id id, boolean big) {
        boolean number = id.number();
        if (number) {
            long value = id.asLong();
            this.writeNumber(value);
        } else {
            byte[] bytes = id.asBytes();
            int len = bytes.length;
            E.checkArgument(len > 0, "Can't write empty id");
            if (!big) {
                E.checkArgument(len <= ID_MAX_LEN,
                                "Id max length is %s, but got %s {%s}",
                                ID_MAX_LEN, len, id);
                len -= 1; // mapping [1, 128] to [0, 127]
                this.writeUInt8(len | 0x80);
            } else {
                E.checkArgument(len <= BIG_ID_MAX_LEN,
                                "Big id max length is %s, but got %s",
                                BIG_ID_MAX_LEN, len);
                len -= 1;
                int high = len >> 8;
                int low = len & 0xff;
                this.writeUInt8(high | 0x80);
                this.writeUInt8(low);
            }
            this.write(bytes);
        }
        return this;
    }

    public Id readId() {
        return this.readId(false);
    }

    public Id readId(boolean big) {
        int b = this.readUInt8();
        boolean number = (b & 0x80) == 0;
        int len = b & 0x7f;
        if (number) {
            return IdGenerator.of(this.readNumber(len));
        } else {
            if (big) {
                int high = len << 8;
                int low = this.readUInt8();
                len = high + low;
            }
            byte[] id = this.read(len + 1);
            return IdGenerator.of(StringEncoding.decode(id));
        }
    }

    public BinaryId asId() {
        int start = this.buffer.position();
        Id id = this.readId();
        int end = this.buffer.position();
        int len = end - start;
        byte[] bytes = new byte[len];
        System.arraycopy(this.array(), start, bytes, 0, len);
        return new BinaryId(bytes, id);
    }

    private void writeNumber(long val) {
        if (Byte.MIN_VALUE <= val && val <= Byte.MAX_VALUE) {
            this.writeUInt8(1);
            this.write((byte) val);
        } else if (Short.MIN_VALUE <= val && val <= Short.MAX_VALUE) {
            this.writeUInt8(2);
            this.writeShort((short) val);
        } else if (Integer.MIN_VALUE <= val && val <= Integer.MAX_VALUE) {
            this.writeUInt8(4);
            this.writeInt((int) val);
        } else {
            this.writeUInt8(8);
            this.writeLong(val);
        }
    }

    private long readNumber(int len) {
        if (len <= 1) {
            return this.read();
        } else if (len <= 2) {
            return this.readShort();
        } else if (len <= 4) {
            return this.readInt();
        } else {
            assert len == 8 : len;
            return this.readLong();
        }
    }
}
