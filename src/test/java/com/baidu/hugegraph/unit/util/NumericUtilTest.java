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

package com.baidu.hugegraph.unit.util;

import java.math.BigDecimal;
import java.util.Date;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.NumericUtil;

public class NumericUtilTest extends BaseUnitTest {

    @Test
    public void testNumberToSortableBytes() {
        // byte
        byte[] bytes = NumericUtil.numberToSortableBytes((byte) 0x33);
        assertEquals(new byte[]{(byte) 0xb3}, bytes);

        bytes = NumericUtil.numberToSortableBytes(Byte.MIN_VALUE);
        assertEquals(new byte[]{0x00}, bytes);

        bytes = NumericUtil.numberToSortableBytes(Byte.MAX_VALUE);
        assertEquals(new byte[]{(byte) 0xff}, bytes);

        bytes = NumericUtil.numberToSortableBytes((byte) -1);
        assertEquals(new byte[]{(byte) 0x7f}, bytes);

        bytes = NumericUtil.numberToSortableBytes((byte) 0);
        assertEquals(new byte[]{(byte) 0x80}, bytes);

        // short
        bytes = NumericUtil.numberToSortableBytes((short) 0x11223344);
        assertEquals(new byte[]{(byte) 0x80, 0x00, 0x33, 0x44}, bytes);

        // int
        bytes = NumericUtil.numberToSortableBytes(0x11223344);
        assertEquals(new byte[]{(byte) 0x91, 0x22, 0x33, 0x44}, bytes);

        // long
        bytes = NumericUtil.numberToSortableBytes(0x1122334455L);
        assertEquals(new byte[]{(byte) 0x80, 0, 0, 0x11,
                                0x22, 0x33, 0x44, 0x55}, bytes);

        // float
        bytes = NumericUtil.numberToSortableBytes(3.14f);
        assertEquals(new byte[]{(byte) 0xc0, 0x48, (byte) 0xf5, (byte) 0xc3},
                     bytes);

        // double
        bytes = NumericUtil.numberToSortableBytes(3.1415926d);
        assertEquals(new byte[]{(byte) 0xc0, 0x09, 0x21, (byte) 0xfb,
                                0x4d, 0x12, (byte) 0xd8, 0x4a}, bytes);

        // BigDecimal
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.numberToSortableBytes(new BigDecimal(123));
        });
    }

    @Test
    public void testSortableBytesToNumber() {
        // byte
        Number value = NumericUtil.sortableBytesToNumber(new byte[]{0x33},
                                                         Byte.class);
        Assert.assertEquals(value, (byte) 0xb3);

        value = NumericUtil.sortableBytesToNumber(new byte[]{(byte) 0x00},
                                                  Byte.class);
        Assert.assertEquals(value, Byte.MIN_VALUE);

        value = NumericUtil.sortableBytesToNumber(new byte[]{(byte) 0xff},
                                                  Byte.class);
        Assert.assertEquals(value, Byte.MAX_VALUE);

        value = NumericUtil.sortableBytesToNumber(new byte[]{(byte) 0x7f},
                                                  Byte.class);
        Assert.assertEquals(value, (byte) -1);

        value = NumericUtil.sortableBytesToNumber(new byte[]{(byte) 0x80},
                                                  Byte.class);
        Assert.assertEquals(value, (byte) 0);

        // short
        value = NumericUtil.sortableBytesToNumber(new byte[]{0, 0, 0x33, 0x44},
                                                  Short.class);
        Assert.assertEquals((short) 0x3344, value);

        // int
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{(byte) 0x91, 0x22, 0x33, 0x44}, Integer.class);
        Assert.assertEquals(0x11223344, value);

        // long
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{(byte) 0x80, 0, 0, 0x11, 0x22, 0x33, 0x44, 0x55},
                Long.class);
        Assert.assertEquals(0x1122334455L, value);

        // float
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{(byte) 0xc0, 0x48, (byte) 0xf5, (byte) 0xc3},
                Float.class);
        Assert.assertEquals(3.14f, value);

        // double
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{(byte) 0xc0, 0x09, 0x21, (byte) 0xfb,
                           0x4d, 0x12, (byte) 0xd8, 0x4a},
                Double.class);
        Assert.assertEquals(3.1415926d, value);

        // BigDecimal
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.sortableBytesToNumber(new byte[123], BigDecimal.class);
        });
    }

    @Test
    public void testIntToSortableBytesAndCompare() {
        byte[] bytes1 = NumericUtil.numberToSortableBytes(123456);
        byte[] bytes2 = NumericUtil.numberToSortableBytes(123456);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(1);
        bytes2 = NumericUtil.numberToSortableBytes(2);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(666);
        bytes2 = NumericUtil.numberToSortableBytes(88);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Integer.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(0);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-123456);
        bytes2 = NumericUtil.numberToSortableBytes(-123456);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(-1);
        bytes2 = NumericUtil.numberToSortableBytes(-2);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-666);
        bytes2 = NumericUtil.numberToSortableBytes(-88);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(Integer.MIN_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(0);
        bytes2 = NumericUtil.numberToSortableBytes(-1);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(0);
        bytes2 = NumericUtil.numberToSortableBytes(Integer.MIN_VALUE);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(1);
        bytes2 = NumericUtil.numberToSortableBytes(-1);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Integer.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);
    }

    @Test
    public void testLongToSortableBytesAndCompare() {
        byte[] bytes1 = NumericUtil.numberToSortableBytes(123456L);
        byte[] bytes2 = NumericUtil.numberToSortableBytes(123456L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(1L);
        bytes2 = NumericUtil.numberToSortableBytes(2L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(666L);
        bytes2 = NumericUtil.numberToSortableBytes(88L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Long.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(0L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-123456L);
        bytes2 = NumericUtil.numberToSortableBytes(-123456L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(-1L);
        bytes2 = NumericUtil.numberToSortableBytes(-2L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-666L);
        bytes2 = NumericUtil.numberToSortableBytes(-88L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(Long.MIN_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(0L);
        bytes2 = NumericUtil.numberToSortableBytes(-1L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(0L);
        bytes2 = NumericUtil.numberToSortableBytes(Long.MIN_VALUE);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(1L);
        bytes2 = NumericUtil.numberToSortableBytes(-1L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Long.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1L);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);
    }

    @Test
    public void testFloatToSortableBytesAndCompare() {
        byte[] bytes1 = NumericUtil.numberToSortableBytes(123456F);
        byte[] bytes2 = NumericUtil.numberToSortableBytes(123456F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(1F);
        bytes2 = NumericUtil.numberToSortableBytes(2F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(666F);
        bytes2 = NumericUtil.numberToSortableBytes(88F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Float.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(0F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-123456F);
        bytes2 = NumericUtil.numberToSortableBytes(-123456F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(-1F);
        bytes2 = NumericUtil.numberToSortableBytes(-2F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-666F);
        bytes2 = NumericUtil.numberToSortableBytes(-88F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(-Float.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(0F);
        bytes2 = NumericUtil.numberToSortableBytes(-1F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(0F);
        bytes2 = NumericUtil.numberToSortableBytes(-Float.MAX_VALUE);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(1F);
        bytes2 = NumericUtil.numberToSortableBytes(-1F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Float.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);
    }

    @Test
    public void testDoubleToSortableBytesAndCompare() {
        byte[] bytes1 = NumericUtil.numberToSortableBytes(123456D);
        byte[] bytes2 = NumericUtil.numberToSortableBytes(123456D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(1D);
        bytes2 = NumericUtil.numberToSortableBytes(2D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(666D);
        bytes2 = NumericUtil.numberToSortableBytes(88D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Double.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(0D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-123456D);
        bytes2 = NumericUtil.numberToSortableBytes(-123456D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) == 0);

        bytes1 = NumericUtil.numberToSortableBytes(-1D);
        bytes2 = NumericUtil.numberToSortableBytes(-2D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(-666D);
        bytes2 = NumericUtil.numberToSortableBytes(-88D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(-Double.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) < 0);

        bytes1 = NumericUtil.numberToSortableBytes(0D);
        bytes2 = NumericUtil.numberToSortableBytes(-1D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(0D);
        bytes2 = NumericUtil.numberToSortableBytes(-Double.MAX_VALUE);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(1D);
        bytes2 = NumericUtil.numberToSortableBytes(-1D);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);

        bytes1 = NumericUtil.numberToSortableBytes(Double.MAX_VALUE);
        bytes2 = NumericUtil.numberToSortableBytes(-1F);
        Assert.assertTrue(Bytes.compare(bytes1, bytes2) > 0);
    }

    @Test
    public void testMinValueOf() {
        Assert.assertEquals(Byte.MIN_VALUE,
                            NumericUtil.minValueOf(Byte.class));

        Assert.assertEquals(Integer.MIN_VALUE,
                            NumericUtil.minValueOf(Short.class));
        Assert.assertEquals(Integer.MIN_VALUE,
                            NumericUtil.minValueOf(Integer.class));
        Assert.assertEquals(Integer.MIN_VALUE,
                            NumericUtil.minValueOf(Float.class));

        Assert.assertEquals(Long.MIN_VALUE,
                            NumericUtil.minValueOf(Long.class));
        Assert.assertEquals(Long.MIN_VALUE,
                            NumericUtil.minValueOf(Double.class));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.minValueOf(null);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.minValueOf(Character.class);
        });
    }

    @Test
    public void testMaxValueOf() {
        Assert.assertEquals(Byte.MAX_VALUE,
                            NumericUtil.maxValueOf(Byte.class));

        Assert.assertEquals(Integer.MAX_VALUE,
                            NumericUtil.maxValueOf(Short.class));
        Assert.assertEquals(Integer.MAX_VALUE,
                            NumericUtil.maxValueOf(Integer.class));
        Assert.assertEquals(Integer.MAX_VALUE,
                            NumericUtil.maxValueOf(Float.class));

        Assert.assertEquals(Long.MAX_VALUE,
                            NumericUtil.maxValueOf(Long.class));
        Assert.assertEquals(Long.MAX_VALUE,
                            NumericUtil.maxValueOf(Double.class));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.maxValueOf(null);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.maxValueOf(Character.class);
        });
    }

    @Test
    public void testIsNumber() {
        Assert.assertEquals(true, NumericUtil.isNumber(byte.class));
        Assert.assertEquals(true, NumericUtil.isNumber(Byte.class));
        Assert.assertEquals(true, NumericUtil.isNumber(short.class));
        Assert.assertEquals(true, NumericUtil.isNumber(Short.class));
        Assert.assertEquals(true, NumericUtil.isNumber(int.class));
        Assert.assertEquals(true, NumericUtil.isNumber(Integer.class));
        Assert.assertEquals(true, NumericUtil.isNumber(long.class));
        Assert.assertEquals(true, NumericUtil.isNumber(Long.class));
        Assert.assertEquals(true, NumericUtil.isNumber(float.class));
        Assert.assertEquals(true, NumericUtil.isNumber(Float.class));
        Assert.assertEquals(true, NumericUtil.isNumber(double.class));
        Assert.assertEquals(true, NumericUtil.isNumber(Double.class));

        Assert.assertEquals(false, NumericUtil.isNumber(char.class));
        Assert.assertEquals(false, NumericUtil.isNumber(Character.class));

        Assert.assertEquals(true, NumericUtil.isNumber(1));
        Assert.assertEquals(true, NumericUtil.isNumber(1L));
        Assert.assertEquals(true, NumericUtil.isNumber(1.0f));
        Assert.assertEquals(true, NumericUtil.isNumber(1.0d));
        Assert.assertEquals(false, NumericUtil.isNumber('1'));
        Assert.assertEquals(false, NumericUtil.isNumber((Object) null));
    }

    @Test
    public void testConvertToNumber() {
        Assert.assertEquals(1, NumericUtil.convertToNumber(1));
        Assert.assertEquals(1.2, NumericUtil.convertToNumber(1.2));

        Assert.assertEquals(new BigDecimal(1.25),
                            NumericUtil.convertToNumber("1.25"));

        Date date = new Date();
        Assert.assertEquals(date.getTime(), NumericUtil.convertToNumber(date));

        Assert.assertEquals(null, NumericUtil.convertToNumber(null));
    }

    @Test
    public void testCompareNumber() {
        Assert.assertEquals(0, NumericUtil.compareNumber(2, 2));
        Assert.assertEquals(1, NumericUtil.compareNumber(10, 2));
        Assert.assertEquals(-1, NumericUtil.compareNumber(1, 2));

        Assert.assertEquals(-1, NumericUtil.compareNumber("1", 2));
        Assert.assertEquals(0, NumericUtil.compareNumber("2.0", 2));
        Assert.assertEquals(1, NumericUtil.compareNumber("2.00001", 2));
        Assert.assertEquals(1, NumericUtil.compareNumber("3.8", 2));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.compareNumber(null, 1);
        }, e -> {
            Assert.assertContains("The first parameter can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.compareNumber(2, null);
        }, e -> {
            Assert.assertContains("The second parameter can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.compareNumber("2", null);
        }, e -> {
            Assert.assertContains("The second parameter can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.compareNumber("f", 2);
        }, e -> {
            Assert.assertContains("Can't compare between 'f' and '2'",
                                  e.getMessage());
        });
    }

    private static void assertEquals(byte[] bytes1, byte[] bytes2) {
        Assert.assertTrue(Bytes.toHex(bytes1) + " != " + Bytes.toHex(bytes2),
                          Bytes.equals(bytes1, bytes2));
    }
}
