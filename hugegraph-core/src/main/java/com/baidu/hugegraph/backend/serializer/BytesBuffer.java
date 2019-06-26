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
import com.baidu.hugegraph.backend.id.Id.IdType;
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

    // NOTE: +1 to let code 0 represent length 1
    public static final int ID_MAX_LEN = UINT8_MAX & 0x7e + 1; // 127
    public static final int BIG_ID_MAX_LEN = UINT16_MAX & 0x7eff + 1; // 32512

    public static final long ID_MIN = Long.MIN_VALUE >> 3;
    public static final long ID_MAX = Long.MAX_VALUE >> 3;
    public static final long ID_MASK = 0x0fffffffffffffffL;

    // The value must be in range [8, 127(ID_MAX_LEN)]
    public static final int INDEX_ID_MAX_LENGTH = 32;

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

    public BytesBuffer copyFrom(BytesBuffer other) {
        return this.write(other.bytes());
    }

    public int remaining() {
        return this.buffer.remaining();
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

    public byte peek() {
        return this.buffer.get(this.buffer.position());
    }

    public byte peekLast() {
        return this.buffer.get(this.buffer.capacity() - 1);
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
        if (id.number()) {
            // Number Id
            long value = id.asLong();
            this.writeNumber(value);
        } else if (id.uuid()) {
            // UUID Id
            byte[] bytes = id.asBytes();
            assert bytes.length == Id.UUID_LENGTH;
            this.writeUInt8(0xff); // 0b11111111 means UUID
            this.write(bytes);
        } else {
            // String Id
            byte[] bytes = id.asBytes();
            int len = bytes.length;
            E.checkArgument(len > 0, "Can't write empty id");
            if (!big) {
                E.checkArgument(len <= ID_MAX_LEN,
                                "Id max length is %s, but got %s {%s}",
                                ID_MAX_LEN, len, id);
                len -= 1; // mapping [1, 127] to [0, 126]
                this.writeUInt8(len | 0x80);
            } else {
                E.checkArgument(len <= BIG_ID_MAX_LEN,
                                "Big id max length is %s, but got %s",
                                BIG_ID_MAX_LEN, len);
                len -= 1;
                int high = len >> 8;
                int low = len & 0xff;
                assert high != 0x7f; // The tail 7 bits 1 reserved for UUID
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
        int b = this.peek();
        boolean number = (b & 0x80) == 0;
        if (number) {
            // Number Id
            return IdGenerator.of(this.readNumber(b));
        } else {
            this.readUInt8();
            int len = b & 0x7f;
            IdType type = IdType.STRING;
            if (len == 0x7f) {
                // UUID Id
                type = IdType.UUID;
                len = Id.UUID_LENGTH;
            } else {
                // String Id
                if (big) {
                    int high = len << 8;
                    int low = this.readUInt8();
                    len = high + low;
                }
                len += 1; // mapping [0, 126] to [1, 127]
            }
            byte[] id = this.read(len);
            return IdGenerator.of(id, type);
        }
    }

    public BytesBuffer writeIndexId(Id id) {
        byte[] bytes = id.asBytes();
        int len = bytes.length;
        E.checkArgument(len > 0, "Can't write empty id");
        E.checkArgument(len <= ID_MAX_LEN,
                        "Id max length is %s, but got %s {%s}",
                        ID_MAX_LEN, len, id);
        this.write(bytes);
        return this;
    }

    public BinaryId readIndexId() {
        int b = this.peekLast();
        int len = b & 0x7f;
        byte[] id = this.read(len + 1);
        return new BinaryId(id, IdGenerator.of(id, false));
    }

    public BinaryId asId() {
        return new BinaryId(this.bytes(), null);
    }

    public BinaryId parseId() {
        // Parse id from bytes
        int start = this.buffer.position();
        Id id = this.readId();
        int end = this.buffer.position();
        int len = end - start;
        byte[] bytes = new byte[len];
        System.arraycopy(this.array(), start, bytes, 0, len);
        return new BinaryId(bytes, id);
    }

    private void writeNumber(long val) {
        int positive = val >= 0 ? 0x10 : 0x00;
        if (Byte.MIN_VALUE <= val && val <= Byte.MAX_VALUE) {
            this.writeUInt8(0x00 | positive);
            this.write((byte) val);
        } else if (Short.MIN_VALUE <= val && val <= Short.MAX_VALUE) {
            this.writeUInt8(0x20 | positive);
            this.writeShort((short) val);
        } else if (Integer.MIN_VALUE <= val && val <= Integer.MAX_VALUE) {
            this.writeUInt8(0x40 | positive);
            this.writeInt((int) val);
        } else {
            E.checkArgument(ID_MIN < val && val < ID_MAX,
                            "Id value must be in [%s, %s], but got %s",
                            ID_MIN, ID_MAX, val);
            this.writeLong((val & ID_MASK) | ((0x60L | positive) << 56));
        }
    }

    private long readNumber(int b) {
        // Parse length from byte 0b0llsnnnn: bits `ll` is the number length
        E.checkArgument((b & 0x80) == 0,
                        "Not a number type with prefix byte '0x%s'",
                        Integer.toHexString(b));
        int length = b >> 5;
        boolean positive = (b & 0x10) > 0;
        switch (length) {
            case 0:
                this.read();
                return this.read();
            case 1:
                this.read();
                return this.readShort();
            case 2:
                this.read();
                return this.readInt();
            case 3:
                long value = this.readLong();
                value &= ID_MASK;
                if (!positive) {
                    value |= Long.MIN_VALUE;
                }
                return value;
            default:
                throw new AssertionError("Invalid length of number: " + length);
        }
    }
}
