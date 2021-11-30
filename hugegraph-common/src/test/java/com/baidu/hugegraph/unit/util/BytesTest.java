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

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.NumericUtil;

public class BytesTest extends BaseUnitTest {

    @Test
    public void testBytesEquals() {
        Assert.assertTrue(Bytes.equals(b("12345678"),
                                       b("12345678")));
        Assert.assertTrue(Bytes.equals(new byte[]{1, 3, 5, 7},
                                       new byte[]{1, 3, 5, 7}));

        Assert.assertFalse(Bytes.equals(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 6, 7}));
        Assert.assertFalse(Bytes.equals(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 5, 7, 0}));
    }

    @Test
    public void testBytesPrefixWith() {
        Assert.assertTrue(Bytes.prefixWith(b("12345678"), b("12345678")));
        Assert.assertTrue(Bytes.prefixWith(b("12345678"), b("1234567")));

        Assert.assertTrue(Bytes.prefixWith(new byte[]{1, 3, 5, 7},
                                           new byte[]{1, 3, 5, 7}));
        Assert.assertTrue(Bytes.prefixWith(new byte[]{1, 3, 5, 7},
                                           new byte[]{1, 3, 5}));
        Assert.assertTrue(Bytes.prefixWith(new byte[]{1, 3, 5, 7},
                                           new byte[]{1, 3}));

        Assert.assertFalse(Bytes.prefixWith(new byte[]{1, 3, 5, 7},
                                            new byte[]{1, 3, 6, 6}));
        Assert.assertFalse(Bytes.prefixWith(new byte[]{1, 3, 5, 7},
                                            new byte[]{3, 1}));
        Assert.assertFalse(Bytes.prefixWith(new byte[]{1, 3, 5, 7},
                                            new byte[]{1, 3, 5, 7, 0}));
    }

    @Test
    public void testBytesCompare() {
        Assert.assertTrue(Bytes.compare(b("12345678"), b("12345678")) == 0);
        Assert.assertTrue(Bytes.compare(b("12345678"), b("1234567")) > 0);
        Assert.assertTrue(Bytes.compare(b("12345678"), b("12345679")) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 5, 7}) == 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 5, 6}) > 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 5}) > 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 0},
                                        new byte[]{1, 3, 5}) > 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3}) > 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 6, 0}) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 4}) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{3, 1}) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 5, 7, 0}) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 7},
                                        new byte[]{1, 3, 5, -1}) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, 0},
                                        new byte[]{1, 3, 5, -128}) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, -2},
                                        new byte[]{1, 3, 5, -1}) < 0);

        Assert.assertTrue(Bytes.compare(new byte[]{1, 3, 5, -128},
                                        new byte[]{1, 3, 5, -1}) < 0);
    }

    @Test
    public void testBytesConcat() {
        Assert.assertArrayEquals(b("12345678"),
                                 Bytes.concat(b("1234"), b("5678")));
        Assert.assertArrayEquals(b("12345678"),
                                 Bytes.concat(b("12345678"), b("")));
        Assert.assertArrayEquals(b("12345678"),
                                 Bytes.concat(b(""), b("12345678")));
    }

    @Test
    public void testBytesContains() {
        Assert.assertTrue(Bytes.contains(b("1234"), (byte) '1'));
        Assert.assertTrue(Bytes.contains(b("1234"), (byte) '3'));
        Assert.assertTrue(Bytes.contains(b("1234"), (byte) '4'));

        Assert.assertFalse(Bytes.contains(b("1234"), (byte) '0'));
        Assert.assertFalse(Bytes.contains(b(""), (byte) '0'));
    }

    @Test
    public void testBytesIndexOf() {
        Assert.assertEquals(0, Bytes.indexOf(b("1234"), (byte) '1'));
        Assert.assertEquals(2, Bytes.indexOf(b("1234"), (byte) '3'));
        Assert.assertEquals(3, Bytes.indexOf(b("1234"), (byte) '4'));

        Assert.assertEquals(-1, Bytes.indexOf(b("1234"), (byte) '0'));
        Assert.assertEquals(-1, Bytes.indexOf(b(""), (byte) '0'));
    }

    @Test
    public void testByteToHex() {
        byte value = 0;
        Assert.assertEquals("00", Bytes.toHex(value));

        value = 127;
        Assert.assertEquals("7f", Bytes.toHex(value));

        value = -128;
        Assert.assertEquals("80", Bytes.toHex(value));

        value = -1;
        Assert.assertEquals("ff", Bytes.toHex(value));
    }

    @Test
    public void testBytesToHex() {
        int value = 0x0103807f;
        byte[] bytes = NumericUtil.intToBytes(value);
        Assert.assertEquals("0103807f", Bytes.toHex(bytes));
    }

    @Test
    public void testBytesFromHex() {
        Assert.assertEquals(0x0103807f,
                            NumericUtil.bytesToInt(Bytes.fromHex("0103807f")));
    }

    private static byte[] b(String string) {
        return string.getBytes();
    }
}
