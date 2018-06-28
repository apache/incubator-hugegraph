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

package com.baidu.hugegraph.util;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.function.Function;

/**
 * This file is copied verbatim from Apache Lucene NumericUtils.java Only the
 * double/float to sortable long/int conversions are retained.
 */
public final class NumericUtil {

    private NumericUtil() {
    }

    /**
     * Converts a <code>double</code> value to a sortable signed
     * <code>long</code>. The value is converted by getting their IEEE 754
     * floating-point &quot;double format&quot; bit layout and then some bits
     * are swapped, to be able to compare the result as long. By this the
     * precision is not reduced, but the value can easily used as a long. The
     * sort order (including {@link Double#NaN}) is defined by
     * {@link Double#compareTo}; {@code NaN} is greater than positive infinity.
     * @param val input double value
     * @return output sortable long value
     * @see #sortableLongToDouble
     */
    public static long doubleToSortableLong(double val) {
        return sortableDoubleBits(Double.doubleToLongBits(val));
    }

    /**
     * Converts a sortable <code>long</code> back to a <code>double</code>.
     * @param val input double value
     * @return output sortable long value
     * @see #doubleToSortableLong
     */
    public static double sortableLongToDouble(long val) {
        return Double.longBitsToDouble(sortableDoubleBits(val));
    }

    /**
     * Converts a <code>float</code> value to a sortable signed
     * <code>int</code>. The value is converted by getting their IEEE 754
     * floating-point &quot;float format&quot; bit layout and then some bits are
     * swapped, to be able to compare the result as int. By this the precision
     * is not reduced, but the value can easily used as an int. The sort order
     * (including {@link Float#NaN}) is defined by {@link Float#compareTo};
     * {@code NaN} is greater than positive infinity.
     * @param val input float value
     * @return output sortable int value
     * @see #sortableIntToFloat
     */
    public static int floatToSortableInt(float val) {
        return sortableFloatBits(Float.floatToIntBits(val));
    }

    /**
     * Converts a sortable <code>int</code> back to a <code>float</code>.
     * @param val input int value
     * @return output sortable float value
     * @see #floatToSortableInt
     */
    public static float sortableIntToFloat(int val) {
        return Float.intBitsToFloat(sortableFloatBits(val));
    }

    /**
     * Converts IEEE 754 representation of a double to sortable order (or back
     * to the original)
     * @param bits The long format of a double value
     * @return The sortable long value
     */
    public static long sortableDoubleBits(long bits) {
        return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
    }

    /**
     * Converts IEEE 754 representation of a float to sortable order (or back to
     * the original)
     * @param bits The int format of an float value
     * @return The sortable int value
     */
    public static int sortableFloatBits(int bits) {
        return bits ^ (bits >> 31) & 0x7fffffff;
    }

    public static byte[] numberToSortableBytes(Number number) {
        if (number instanceof Long) {
            return longToBytes(number.longValue());
        } else if (number instanceof Double) {
            return longToBytes(doubleToSortableLong(number.doubleValue()));
        } else if (number instanceof Float) {
            return intToBytes(floatToSortableInt(number.floatValue()));
        } else if (number instanceof Integer || number instanceof Short) {
            return intToBytes(number.intValue());
        } else if (number instanceof Byte) {
            return new byte[]{number.byteValue()} ;
        }

        // TODO: support other number types
        return null;
    }

    public static Number sortableBytesToNumber(byte[] bytes, Class<?> clazz) {
        assert NumericUtil.isNumber(clazz);

        if (clazz == Long.class) {
            return bytesToLong(bytes);
        } else if (clazz == Double.class) {
            return sortableLongToDouble(bytesToLong(bytes));
        } else if (clazz == Float.class) {
            return sortableIntToFloat(bytesToInt(bytes));
        } else if (clazz == Integer.class) {
            return bytesToInt(bytes);
        } else if (clazz == Short.class) {
            return (short) bytesToInt(bytes);
        } else if (clazz == Byte.class) {
            return bytes[0];
        }

        // TODO: support other number types
        return null;
    }

    public static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        assert bytes.length == Long.BYTES;
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static byte[] intToBytes(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return buffer.array();
    }

    public static int bytesToInt(byte[] bytes) {
        assert bytes.length == Integer.BYTES;
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static boolean isNumber(Object value) {
        if (value == null) {
            return false;
        }
        return isNumber(value.getClass());
    }

    public static boolean isNumber(Class<?> clazz) {
        return Number.class.isAssignableFrom(clazz);
    }

    public static Object convertToNumber(Object value) {
        if (!isNumber(value) && value != null) {
            if (value instanceof Date) {
                value = ((Date) value).getTime();
            } else {
                // TODO: add some more types to convert
                value = new BigDecimal(value.toString());
            }
        }
        return value;
    }

    /**
     * Compare object with a number, the object should be a number,
     * or it can be converted to a BigDecimal
     * @param first     might be number or string
     * @param second    must be number
     * @return          the value 0 if first is numerically equal to second;
     *                  a value less than 0 if first is numerically less than
     *                  second; and a value greater than 0 if first is
     *                  numerically greater than second.
     */
    @SuppressWarnings("unchecked")
    public static int compareNumber(Object first, Number second) {
        if (first instanceof Number && first instanceof Comparable &&
            first.getClass().equals(second.getClass())) {
            return ((Comparable<Number>) first).compareTo(second);
        }

        Function<Object, BigDecimal> toBig = (number) -> {
            try {
                return new BigDecimal(number.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                          "Can't compare between %s and %s, " +
                          "they must be numbers", first, second));
            }
        };

        BigDecimal n1 = toBig.apply(first);
        BigDecimal n2 = toBig.apply(second);

        return n1.compareTo(n2);
    }
}
