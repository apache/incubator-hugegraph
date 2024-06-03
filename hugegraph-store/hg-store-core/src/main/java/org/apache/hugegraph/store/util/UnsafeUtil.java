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

package org.apache.hugegraph.store.util;

import java.lang.reflect.Field;

import lombok.extern.slf4j.Slf4j;

/**
 * TODO: refer license later, 76% match, maybe refer to jraft-core (1.2.6)
 */
@Slf4j
public class UnsafeUtil {

    private static final Object UNSAFE = getUnsafe0();
    private static final UnsafeAccessor UNSAFE_ACCESSOR = getUnsafeAccessor0();

    private static final long BYTE_ARRAY_BASE_OFFSET = arrayBaseOffset(byte[].class);
    private static final long STRING_VALUE_OFFSET = objectFieldOffset(stringValueField());

    public static boolean hasUnsafe() {
        return UNSAFE != null;
    }

    public static Object getUnsafe0() {
        Object unsafe;
        try {
            final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            final Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = unsafeField.get(null);
        } catch (final Throwable t) {
            if (log.isWarnEnabled()) {
                log.warn("sun.misc.Unsafe.theUnsafe: unavailable.", t);
            }
            unsafe = null;
        }
        return unsafe;
    }

    public static void putByte(final byte[] target, final long index, final byte value) {
        UNSAFE_ACCESSOR.putByte(target, BYTE_ARRAY_BASE_OFFSET + index, value);
    }

    public static byte getByte(final byte[] target, final long index) {
        return UNSAFE_ACCESSOR.getByte(target, BYTE_ARRAY_BASE_OFFSET + index);
    }

    public static int arrayBaseOffset(final Class<?> clazz) {
        return hasUnsafe() ? UNSAFE_ACCESSOR.arrayBaseOffset(clazz) : -1;
    }

    public static String moveToString(final char[] chars) {
        if (STRING_VALUE_OFFSET == -1) {
            // In the off-chance that this JDK does not implement String as we'd expect, just do
            // a copy.
            return new String(chars);
        }
        final String str;
        try {
            str = (String) UNSAFE_ACCESSOR.allocateInstance(String.class);
        } catch (final InstantiationException e) {
            // This should never happen, but return a copy as a fallback just in case.
            return new String(chars);
        }
        UNSAFE_ACCESSOR.putObject(str, STRING_VALUE_OFFSET, chars);
        return str;
    }

    public static long objectFieldOffset(final Field field) {
        return field == null || hasUnsafe() ? UNSAFE_ACCESSOR.objectFieldOffset(field) : -1;
    }

    private static Field stringValueField() {
        return field(String.class, "value", char[].class);
    }

    private static Field field(final Class<?> clazz, final String fieldName,
                               final Class<?> expectedType) {
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            if (!field.getType().equals(expectedType)) {
                return null;
            }
        } catch (final Throwable t) {
            // Failed to access the fields.
            field = null;
        }
        return field;
    }

    private static UnsafeAccessor getUnsafeAccessor0() {
        return hasUnsafe() ? new UnsafeAccessor(UNSAFE) : null;
    }

    public static class UnsafeAccessor {

        private final sun.misc.Unsafe unsafe;

        public UnsafeAccessor(Object unsafe) {
            this.unsafe = (sun.misc.Unsafe) unsafe;
        }

        /**
         * Returns the {@link sun.misc.Unsafe}'s instance.
         */
        public sun.misc.Unsafe getUnsafe() {
            return unsafe;
        }

        public byte getByte(final Object target, final long offset) {
            return this.unsafe.getByte(target, offset);
        }

        public void putByte(final Object target, final long offset, final byte value) {
            this.unsafe.putByte(target, offset, value);
        }

        public short getShort(final Object target, final long offset) {
            return this.unsafe.getShort(target, offset);
        }

        public void putShort(final Object target, final long offset, final short value) {
            this.unsafe.putShort(target, offset, value);
        }

        public int getInt(final Object target, final long offset) {
            return this.unsafe.getInt(target, offset);
        }

        public void putInt(final Object target, final long offset, final int value) {
            this.unsafe.putInt(target, offset, value);
        }

        public long getLong(final Object target, final long offset) {
            return this.unsafe.getLong(target, offset);
        }

        public void putLong(final Object target, final long offset, final long value) {
            this.unsafe.putLong(target, offset, value);
        }

        public boolean getBoolean(final Object target, final long offset) {
            return this.unsafe.getBoolean(target, offset);
        }

        public void putBoolean(final Object target, final long offset, final boolean value) {
            this.unsafe.putBoolean(target, offset, value);
        }

        public float getFloat(final Object target, final long offset) {
            return this.unsafe.getFloat(target, offset);
        }

        public void putFloat(final Object target, final long offset, final float value) {
            this.unsafe.putFloat(target, offset, value);
        }

        public double getDouble(final Object target, final long offset) {
            return this.unsafe.getDouble(target, offset);
        }

        public void putDouble(final Object target, final long offset, final double value) {
            this.unsafe.putDouble(target, offset, value);
        }

        public Object getObject(final Object target, final long offset) {
            return this.unsafe.getObject(target, offset);
        }

        public void putObject(final Object target, final long offset, final Object value) {
            this.unsafe.putObject(target, offset, value);
        }

        public byte getByte(final long address) {
            return this.unsafe.getByte(address);
        }

        public void putByte(final long address, final byte value) {
            this.unsafe.putByte(address, value);
        }

        public short getShort(final long address) {
            return this.unsafe.getShort(address);
        }

        public void putShort(final long address, final short value) {
            this.unsafe.putShort(address, value);
        }

        public int getInt(final long address) {
            return this.unsafe.getInt(address);
        }

        public void putInt(final long address, final int value) {
            this.unsafe.putInt(address, value);
        }

        public long getLong(final long address) {
            return this.unsafe.getLong(address);
        }

        public void putLong(final long address, final long value) {
            this.unsafe.putLong(address, value);
        }

        public void copyMemory(final Object srcBase, final long srcOffset, final Object dstBase,
                               final long dstOffset,
                               final long bytes) {
            this.unsafe.copyMemory(srcBase, srcOffset, dstBase, dstOffset, bytes);
        }

        public void copyMemory(final long srcAddress, final long dstAddress, final long bytes) {
            this.unsafe.copyMemory(srcAddress, dstAddress, bytes);
        }

        public byte getByteVolatile(final Object target, final long offset) {
            return this.unsafe.getByteVolatile(target, offset);
        }

        public void putByteVolatile(final Object target, final long offset, final byte value) {
            this.unsafe.putByteVolatile(target, offset, value);
        }

        public short getShortVolatile(final Object target, final long offset) {
            return this.unsafe.getShortVolatile(target, offset);
        }

        public void putShortVolatile(final Object target, final long offset, final short value) {
            this.unsafe.putShortVolatile(target, offset, value);
        }

        public int getIntVolatile(final Object target, final long offset) {
            return this.unsafe.getIntVolatile(target, offset);
        }

        public void putIntVolatile(final Object target, final long offset, final int value) {
            this.unsafe.putIntVolatile(target, offset, value);
        }

        public long getLongVolatile(final Object target, final long offset) {
            return this.unsafe.getLongVolatile(target, offset);
        }

        public void putLongVolatile(final Object target, final long offset, final long value) {
            this.unsafe.putLongVolatile(target, offset, value);
        }

        public boolean getBooleanVolatile(final Object target, final long offset) {
            return this.unsafe.getBooleanVolatile(target, offset);
        }

        public void putBooleanVolatile(final Object target, final long offset,
                                       final boolean value) {
            this.unsafe.putBooleanVolatile(target, offset, value);
        }

        public float getFloatVolatile(final Object target, final long offset) {
            return this.unsafe.getFloatVolatile(target, offset);
        }

        public void putFloatVolatile(final Object target, final long offset, final float value) {
            this.unsafe.putFloatVolatile(target, offset, value);
        }

        public double getDoubleVolatile(final Object target, final long offset) {
            return this.unsafe.getDoubleVolatile(target, offset);
        }

        public void putDoubleVolatile(final Object target, final long offset, final double value) {
            this.unsafe.putDoubleVolatile(target, offset, value);
        }

        public Object getObjectVolatile(final Object target, final long offset) {
            return this.unsafe.getObjectVolatile(target, offset);
        }

        public void putObjectVolatile(final Object target, final long offset, final Object value) {
            this.unsafe.putObjectVolatile(target, offset, value);
        }

        /**
         * Reports the offset of the first element in the storage allocation of a
         * given array class.
         */
        public int arrayBaseOffset(final Class<?> clazz) {
            return this.unsafe != null ? this.unsafe.arrayBaseOffset(clazz) : -1;
        }

        /**
         * Reports the scale factor for addressing elements in the storage
         * allocation of a given array class.
         */
        public int arrayIndexScale(final Class<?> clazz) {
            return this.unsafe != null ? this.unsafe.arrayIndexScale(clazz) : -1;
        }

        /**
         * Returns the offset of the provided field, or {@code -1} if {@code sun.misc.Unsafe} is not
         * available.
         */
        public long objectFieldOffset(final Field field) {
            return field == null || this.unsafe == null ? -1 : this.unsafe.objectFieldOffset(field);
        }

        public Object allocateInstance(final Class<?> clazz) throws InstantiationException {
            return this.unsafe.allocateInstance(clazz);
        }

        public void throwException(final Throwable t) {
            this.unsafe.throwException(t);
        }
    }
}
