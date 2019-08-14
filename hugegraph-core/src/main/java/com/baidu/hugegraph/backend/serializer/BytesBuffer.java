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
import java.util.Date;
import java.util.UUID;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.Id.IdType;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry.BinaryId;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.KryoUtil;
import com.baidu.hugegraph.util.NumericUtil;
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
    public static final int ID_LEN_MASK = 0x7f;
    public static final int ID_LEN_MAX = UINT8_MAX & 0x7e + 1; // 127
    public static final int BIG_ID_LEN_MAX = UINT16_MAX & 0x7eff + 1; // 32512

    public static final long ID_MIN = Long.MIN_VALUE >> 3;
    public static final long ID_MAX = Long.MAX_VALUE >> 3;
    public static final long ID_MASK = 0x0fffffffffffffffL;

    public static final byte STRING_ENDING_BYTE = (byte) 0xff;

    // The value must be in range [8, 127(ID_LEN_MAX)]
    public static final int INDEX_HASH_ID_THRESHOLD = 32;

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

    public BytesBuffer flip() {
        this.buffer.flip();
        return this;
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

    public BytesBuffer write(byte[] val, int offset, int length) {
        require(BYTE_LEN * length);
        this.buffer.put(val, offset, length);
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

    public BytesBuffer writeBytes(byte[] bytes) {
        E.checkArgument(bytes.length <= UINT16_MAX,
                        "The max length of bytes is %s, got %s",
                        UINT16_MAX, bytes.length);
        require(SHORT_LEN + bytes.length);
        this.writeUInt16(bytes.length);
        this.write(bytes);
        return this;
    }

    public byte[] readBytes() {
        int length = this.readUInt16();
        byte[] bytes = this.read(length);
        return bytes;
    }

    public BytesBuffer writeStringRaw(String val) {
        this.write(StringEncoding.encode(val));
        return this;
    }

    public BytesBuffer writeString(String val) {
        byte[] bytes = StringEncoding.encode(val);
        this.writeBytes(bytes);
        return this;
    }

    public String readString() {
        return StringEncoding.decode(this.readBytes());
    }

    public BytesBuffer writeStringWithEnding(String val) {
        byte[] bytes = StringEncoding.encode(val);
        this.write(bytes);
        /*
         * A reasonable ending symbol should be 0x00(to ensure order), but
         * considering that some backends like PG do not support 0x00 string,
         * so choose 0xFF currently.
         */
        this.write(STRING_ENDING_BYTE);
        return this;
    }

    public String readStringWithEnding() {
        return StringEncoding.decode(this.readBytesWithEnding());
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

    public BytesBuffer writeVInt(int value) {
        // NOTE: negative numbers are not compressed
        if(value > 0x0FFFFFFF || value < 0) {
            this.write((byte) (0x80 | ((value >>> 28) & 0x7F)));
        }
        if(value > 0x1FFFFF || value < 0) {
            this.write((byte) (0x80 | ((value >>> 21) & 0x7F)));
        }
        if(value > 0x3FFF || value < 0) {
            this.write((byte) (0x80 | ((value >>> 14) & 0x7F)));
        }
        if(value > 0x7F || value < 0) {
            this.write((byte) (0x80 | ((value >>>  7) & 0x7F)));
        }
        this.write((byte) (value & 0x7F));

        return this;
    }

    public int readVInt() {
        byte leading = this.read();
        E.checkArgument(leading != 0x80,
                        "Unexpected varint with leading byte '0x%s'",
                        Integer.toHexString(leading));
        int value = leading & 0x7F;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i;
        for (i = 0; i < 5; i++) {
            byte b = this.read();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7F) | (value << 7);
            }
        }

        E.checkArgument(i < 5,
                        "Unexpected varint %s with too many bytes(%s)",
                        value, i + 1);
        E.checkArgument(i < 4 || (leading & 0x70) == 0,
                        "Unexpected varint %s with leading byte '0x%s'",
                        value, Integer.toHexString(leading));
        return value;
    }

    public BytesBuffer writeVLong(long value) {
        if(value < 0) {
            this.write((byte) 0x81);
        }
        if(value > 0xFFFFFFFFFFFFFFL || value < 0L) {
            this.write((byte) (0x80 | ((value >>> 56) & 0x7FL)));
        }
        if(value > 0x1FFFFFFFFFFFFL || value < 0L) {
            this.write((byte) (0x80 | ((value >>> 49) & 0x7FL)));
        }
        if(value > 0x3FFFFFFFFFFL || value < 0L) {
            this.write((byte) (0x80 | ((value >>> 42) & 0x7FL)));
        }
        if(value > 0x7FFFFFFFFL || value < 0L) {
            this.write((byte) (0x80 | ((value >>> 35) & 0x7FL)));
        }
        if(value > 0xFFFFFFFL || value < 0L) {
            this.write((byte) (0x80 | ((value >>> 28) & 0x7FL)));
        }
        if(value > 0x1FFFFFL || value < 0L) {
            this.write((byte) (0x80 | ((value >>> 21) & 0x7FL)));
        }
        if(value > 0x3FFFL || value < 0L) {
            this.write((byte) (0x80 | ((value >>> 14) & 0x7FL)));
        }
        if(value > 0x7FL || value < 0L) {
            this.write((byte) (0x80 | ((value >>>  7) & 0x7FL)));
        }
        this.write((byte) (value & 0x7FL));

        return this;
    }

    public long readVLong() {
        byte leading = this.read();
        E.checkArgument(leading != 0x80,
                        "Unexpected varlong with leading byte '0x%s'",
                        Integer.toHexString(leading));
        long value = leading & 0x7FL;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i;
        for (i = 0; i < 10; i++) {
            byte b = this.read();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7F) | (value << 7);
            }
        }

        E.checkArgument(i < 10,
                        "Unexpected varlong %s with too many bytes(%s)",
                        value, i + 1);
        E.checkArgument(i < 9 || (leading & 0x7e) == 0,
                        "Unexpected varlong %s with leading byte '0x%s'",
                        value, Integer.toHexString(leading));
        return value;
    }

    public BytesBuffer writeProperty(PropertyKey pkey, Object value) {
        if (pkey.cardinality() != Cardinality.SINGLE) {
            this.writeBytes(KryoUtil.toKryo(value));
            return this;
        }
        switch (pkey.dataType()) {
            case BOOLEAN:
                this.writeVInt(((Boolean) value) ? 1 : 0);
                break;
            case BYTE:
                this.writeVInt((Byte) value);
                break;
            case INT:
                this.writeVInt((Integer) value);
                break;
            case FLOAT:
                this.writeVInt(NumericUtil.floatToSortableInt((Float) value));
                break;
            case LONG:
                this.writeVLong((Long) value);
                break;
            case DATE:
                this.writeVLong(((Date) value).getTime());
                break;
            case DOUBLE:
                long l = NumericUtil.doubleToSortableLong((Double) value);
                this.writeVLong(l);
                break;
            case TEXT:
                this.writeString((String) value);
                break;
            case BLOB:
                this.writeBytes((byte[]) value);
                break;
            case UUID:
                UUID uuid = (UUID) value;
                // Generally writeVLong(uuid) can't save space
                this.writeLong(uuid.getMostSignificantBits());
                this.writeLong(uuid.getLeastSignificantBits());
                break;
            default:
                this.writeBytes(KryoUtil.toKryo(value));
                break;
        }
        return this;
    }

    public Object readProperty(PropertyKey pkey) {
        if (pkey.cardinality() != Cardinality.SINGLE) {
            return KryoUtil.fromKryo(this.readBytes(),
                                     pkey.implementClazz());
        }
        switch (pkey.dataType()) {
            case BOOLEAN:
                return this.readVInt() == 1;
            case BYTE:
                return (byte) this.readVInt();
            case INT:
                return this.readVInt();
            case FLOAT:
                return NumericUtil.sortableIntToFloat(this.readVInt());
            case LONG:
                return this.readVLong();
            case DATE:
                return new Date(this.readVLong());
            case DOUBLE:
                return NumericUtil.sortableLongToDouble(this.readVLong());
            case TEXT:
                return this.readString();
            case BLOB:
                return this.readBytes();
            case UUID:
                return new UUID(this.readLong(), this.readLong());
            default:
                return KryoUtil.fromKryo(this.readBytes(),
                                         pkey.implementClazz());
        }
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
                E.checkArgument(len <= ID_LEN_MAX,
                                "Id max length is %s, but got %s {%s}",
                                ID_LEN_MAX, len, id);
                len -= 1; // mapping [1, 127] to [0, 126]
                this.writeUInt8(len | 0x80);
            } else {
                E.checkArgument(len <= BIG_ID_LEN_MAX,
                                "Big id max length is %s, but got %s {%s}",
                                BIG_ID_LEN_MAX, len, id);
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
            int len = b & ID_LEN_MASK;
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

    public BytesBuffer writeIndexId(Id id, HugeType type) {
        return this.writeIndexId(id, type, true);
    }

    public BytesBuffer writeIndexId(Id id, HugeType type, boolean withEnding) {
        byte[] bytes = id.asBytes();
        int len = bytes.length;
        E.checkArgument(len > 0, "Can't write empty id");

        this.write(bytes);
        if (type.isStringIndex()) {
            // Not allow '0xff' exist in string index id
            E.checkArgument(!Bytes.contains(bytes, STRING_ENDING_BYTE),
                            "The %s type index id can't contains " +
                            "byte '0x%s', but got: 0x%s", type,
                            Integer.toHexString(STRING_ENDING_BYTE),
                            Bytes.toHex(bytes));
            if (withEnding) {
                this.write(STRING_ENDING_BYTE);
            }
        }
        return this;
    }

    public BinaryId readIndexId(HugeType type) {
        byte[] id;
        if (type.isRange4Index()) {
            // IndexLabel 4 bytes + fieldValue 4 bytes
            id = this.read(8);
        } else if (type.isRange8Index()) {
            // IndexLabel 4 bytes + fieldValue 8 bytes
            id = this.read(12);
        } else {
            assert type.isStringIndex();
            id = this.readBytesWithEnding();
        }
        return new BinaryId(id, IdGenerator.of(id, IdType.STRING));
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
            E.checkArgument(ID_MIN <= val && val <= ID_MAX,
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
                    // Restore the bits of the original negative number
                    value |= ~ID_MASK;
                }
                return value;
            default:
                throw new AssertionError("Invalid length of number: " + length);
        }
    }

    private byte[] readBytesWithEnding() {
        int start = this.buffer.position();
        boolean foundEnding =false;
        byte current;
        while (this.remaining() > 0) {
            current = this.read();
            if (current == STRING_ENDING_BYTE) {
                foundEnding = true;
                break;
            }
        }
        E.checkArgument(foundEnding, "Not found ending '0x%s'",
                        Integer.toHexString(STRING_ENDING_BYTE));
        int end = this.buffer.position() - 1;
        int len = end - start;
        byte[] bytes = new byte[len];
        System.arraycopy(this.array(), start, bytes, 0, len);
        return bytes;
    }
}
