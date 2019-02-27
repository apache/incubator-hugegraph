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
        assertEquals(new byte[]{0x33}, bytes);

        // short
        bytes = NumericUtil.numberToSortableBytes((short) 0x11223344);
        assertEquals(new byte[]{0, 0, 0x33, 0x44}, bytes);

        // int
        bytes = NumericUtil.numberToSortableBytes(0x11223344);
        assertEquals(new byte[]{0x11, 0x22, 0x33, 0x44}, bytes);

        // long
        bytes = NumericUtil.numberToSortableBytes(0x1122334455L);
        assertEquals(new byte[]{0, 0, 0, 0x11, 0x22, 0x33, 0x44, 0x55}, bytes);

        // float
        bytes = NumericUtil.numberToSortableBytes(3.14f);
        assertEquals(new byte[]{0x40, 0x48, (byte) 0xf5, (byte) 0xc3}, bytes);

        // double
        bytes = NumericUtil.numberToSortableBytes(3.1415926d);
        assertEquals(new byte[]{0x40, 0x09, 0x21, (byte) 0xfb,
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
        Assert.assertEquals(value, (byte) 0x33);

        // short
        value = NumericUtil.sortableBytesToNumber(new byte[]{0, 0, 0x33, 0x44},
                                                  Short.class);
        Assert.assertEquals((short) 0x3344, value);

        // int
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{0x11, 0x22, 0x33, 0x44}, Integer.class);
        Assert.assertEquals(0x11223344, value);

        // long
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{0, 0, 0, 0x11, 0x22, 0x33, 0x44, 0x55}, Long.class);
        Assert.assertEquals(0x1122334455L, value);

        // float
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{0x40, 0x48, (byte) 0xf5, (byte) 0xc3}, Float.class);
        Assert.assertEquals(3.14f, value);

        // double
        value = NumericUtil.sortableBytesToNumber(
                new byte[]{0x40, 0x09, 0x21, (byte) 0xfb,
                           0x4d, 0x12, (byte) 0xd8, 0x4a},
                Double.class);
        Assert.assertEquals(3.1415926d, value);

        // BigDecimal
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            NumericUtil.sortableBytesToNumber(new byte[123], BigDecimal.class);
        });
    }

    private static void assertEquals(byte[] bytes1, byte[] bytes2) {
        Assert.assertTrue(Bytes.toHex(bytes1) + " != " + Bytes.toHex(bytes2),
                          Bytes.equals(bytes1, bytes2));
    }
}
