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

package org.apache.hugegraph.store.query;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.store.query.concurrent.AtomicFloat;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * todo: 修改成类型二进制的存储
 */
public class KvSerializer {

    private static final byte TYPE_INT = 0;

    private static final byte TYPE_LONG = 1;

    private static final byte TYPE_FLOAT = 2;

    private static final byte TYPE_DOUBLE = 3;

    private static final byte TYPE_STRING = 4;

    private static final byte TYPE_BIG_DECIMAL = 5;

    /**
     * for avg function
     */
    private static final byte TYPE_TUPLE2 = 6;

    private static final byte TYPE_AT_INT = 7;

    private static final byte TYPE_AT_LONG = 8;

    private static final byte TYPE_AT_FLOAT = 9;

    private static final byte TYPE_AT_DOUBLE = 10;

    private static final byte TYPE_NULL = 127;

    public static byte[] toBytes(List<Object> list) {
        ByteBuffer buffer = ByteBuffer.allocate(list == null ? 4 : list.size() * 4 + 4);
        if (list == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(list.size());
            for (Object o : list) {
                buffer = write(buffer, o);
            }
        }

        byte[] bytes = buffer.array();
        int position = buffer.position();
        if (position == bytes.length) {
            return bytes;
        } else {
            return Arrays.copyOf(bytes, position);
        }
    }

    public static List<Comparable> fromBytes(byte[] bytes) {
        List<Comparable> list = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int n = buffer.getInt();
        for (int i = 0; i < n; i++) {
            list.add((Comparable) read(buffer));
        }
        return list;
    }

    public static List<Object> fromObjectBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int n = buffer.getInt();
        if (n == -1) {
            return null;
        }

        List<Object> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(read(buffer));
        }
        return list;
    }

    /**
     * 从ByteBuffer中读取对象并返回。
     *
     * @param buffer 包含要读取对象的ByteBuffer
     * @return 返回从ByteBuffer中读取的对象。如果类型为null，则返回null。
     * @throws RuntimeException 当无法支持的类型时抛出异常。
     */
    private static Object read(ByteBuffer buffer) {
        var b = buffer.get();
        switch (b) {
            case TYPE_INT:
                return readInt(buffer);
            case TYPE_AT_INT:
                return new AtomicInteger(readInt(buffer));
            case TYPE_LONG:
                return readLong(buffer);
            case TYPE_AT_LONG:
                return new AtomicLong(readLong(buffer));
            case TYPE_FLOAT:
                return readFloat(buffer);
            case TYPE_AT_FLOAT:
                return new AtomicFloat(readFloat(buffer));
            case TYPE_DOUBLE:
                return readDouble(buffer);
            case TYPE_AT_DOUBLE:
                return new AtomicDouble(readDouble(buffer));
            case TYPE_STRING:
                return readString(buffer);
            case TYPE_BIG_DECIMAL:
                return readBigDecimal(buffer);
            case TYPE_TUPLE2:
                return readTuple2(buffer);
            case TYPE_NULL:
                return null;
            default:
                throw new RuntimeException("unsupported type " + b);
        }
    }

    /**
     * 写入数据到ByteBuffer中，支持以下类型的写入：
     * <ul>
     * <li><code>null</code></li>
     * <li>{@link Long}</li>
     * <li>{@link AtomicInteger}</li>
     * <li>{@link Float}</li>
     * <li>{@link AtomicFloat}</li>
     * <li>{@link Double}</li>
     * <li>{@link AtomicDouble}</li>
     * <li>{@link String}</li>
     * </ul>
     *
     * @param buffer 当前写入的ByteBuffer
     * @param o      需要写入的数据对象
     * @return 返回更新后的ByteBuffer
     */
    private static ByteBuffer write(ByteBuffer buffer, Object o) {
        if (o == null) {
            buffer = writeByte(buffer, TYPE_NULL);
            return buffer;
        }

        switch (o.getClass().getName()) {
            case "java.lang.Long":
                buffer = writeByte(buffer, TYPE_LONG);
                buffer = writeLong(buffer, (Long) o);
                break;
            case "java.util.concurrent.atomic.AtomicLong":
                buffer = writeByte(buffer, TYPE_AT_LONG);
                buffer = writeLong(buffer, ((AtomicLong) o).get());
                break;
            case "java.lang.Integer":
                buffer = writeByte(buffer, TYPE_INT);
                buffer = writeInt(buffer, (Integer) o);
                break;
            case "java.util.concurrent.atomic.AtomicInteger":
                buffer = writeByte(buffer, TYPE_AT_INT);
                buffer = writeInt(buffer, ((AtomicInteger) o).get());
                break;
            case "java.lang.Float":
                buffer = writeByte(buffer, TYPE_FLOAT);
                buffer = writeFloat(buffer, (Float) o);
                break;
            case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                buffer = writeByte(buffer, TYPE_AT_FLOAT);
                buffer = writeFloat(buffer, ((AtomicFloat) o).get());
                break;
            case "java.lang.Double":
                buffer = writeByte(buffer, TYPE_DOUBLE);
                buffer = writeDouble(buffer, (Double) o);
                break;
            case "com.google.common.util.concurrent.AtomicDouble":
                buffer = writeByte(buffer, TYPE_AT_DOUBLE);
                buffer = writeDouble(buffer, ((AtomicDouble) o).get());
                break;
            case "java.lang.String":
                buffer = writeByte(buffer, TYPE_STRING);
                buffer = writeString(buffer, (String) o);
                break;
            case "java.math.BigDecimal":
                buffer = writeByte(buffer, TYPE_BIG_DECIMAL);
                buffer = writeBigDecimal(buffer, (BigDecimal) o);
                break;
            case "org.apache.hugegraph.store.query.Tuple2":
                buffer = writeByte(buffer, TYPE_TUPLE2);
                buffer = write(buffer, ((Tuple2) o).getV1());
                buffer = write(buffer, ((Tuple2) o).getV2());
                break;
            default:
                throw new RuntimeException("unsupported type " + o.getClass().getName());
        }

        return buffer;
    }

    private static ByteBuffer writeByte(ByteBuffer buffer, byte b) {
        buffer = ensureCapacity(buffer, 1);
        buffer.put(b);
        return buffer;
    }

    private static ByteBuffer writeInt(ByteBuffer buffer, int i) {
        buffer = ensureCapacity(buffer, Integer.BYTES);
        buffer.putInt(i);
        return buffer;
    }

    private static int readInt(ByteBuffer buffer) {
        return buffer.getInt();
    }

    private static ByteBuffer writeLong(ByteBuffer buffer, long l) {
        buffer = ensureCapacity(buffer, Long.BYTES);
        buffer.putLong(l);
        return buffer;
    }

    private static long readLong(ByteBuffer buffer) {
        return buffer.getLong();
    }

    private static ByteBuffer writeFloat(ByteBuffer buffer, float f) {
        buffer = ensureCapacity(buffer, Float.BYTES);
        buffer.putFloat(f);
        return buffer;
    }

    private static float readFloat(ByteBuffer buffer) {
        return buffer.getFloat();
    }

    private static ByteBuffer writeDouble(ByteBuffer buffer, double d) {
        buffer = ensureCapacity(buffer, Double.BYTES);
        buffer.putDouble(d);
        return buffer;
    }

    private static double readDouble(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    private static ByteBuffer writeString(ByteBuffer buffer, String s) {
        byte[] bytes = s.getBytes();
        buffer = ensureCapacity(buffer, bytes.length + Integer.BYTES);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        return buffer;
    }

    private static String readString(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    private static ByteBuffer writeBigDecimal(ByteBuffer buffer, BigDecimal d) {
        return writeString(buffer, d.toString());
    }

    private static BigDecimal readBigDecimal(ByteBuffer buffer) {
        return new BigDecimal(readString(buffer));
    }

    private static Tuple2 readTuple2(ByteBuffer buffer) {
        return Tuple2.of(read(buffer), read(buffer));
    }

    private static ByteBuffer ensureCapacity(ByteBuffer buffer, int capacity) {
        if (buffer.remaining() < capacity) {
            // 防止 capacity 大于 现在的2倍
            var newBuffer = ByteBuffer.allocate(buffer.capacity() * 2 + capacity);
            buffer.flip();
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
        return buffer;
    }
}
