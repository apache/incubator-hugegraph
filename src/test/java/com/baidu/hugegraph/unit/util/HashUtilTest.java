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
import com.baidu.hugegraph.util.HashUtil;

public class HashUtilTest extends BaseUnitTest {

    @Test
    public void testHash() {
        // hash 32 bits (4 bytes)

        String h = HashUtil.hash("");
        Assert.assertEquals(8, h.length());
        Assert.assertEquals("00000000", h);

        h = HashUtil.hash("q");
        Assert.assertEquals(8, h.length());
        Assert.assertEquals("e80982ff", h);

        h = HashUtil.hash("qq");
        Assert.assertEquals(8, h.length());
        Assert.assertEquals("252ef918", h);

        h = HashUtil.hash("qwertyuiop[]asdfghjkl;'zxcvbnm,./");
        Assert.assertEquals(8, h.length());
        Assert.assertEquals("fcc1f9fa", h);
    }

    @Test
    public void testHashWithBytes() {
        // hash 32 bits (4 bytes)

        byte[] h = HashUtil.hash(b(""));
        Assert.assertEquals(4, h.length);
        Assert.assertEquals("00000000", hex(h));

        h = HashUtil.hash(b("q"));
        Assert.assertEquals(4, h.length);
        Assert.assertEquals("e80982ff", hex(h));

        h = HashUtil.hash(b("qq"));
        Assert.assertEquals(4, h.length);
        Assert.assertEquals("252ef918", hex(h));

        h = HashUtil.hash(b("qwertyuiop[]asdfghjkl;'zxcvbnm,./"));
        Assert.assertEquals(4, h.length);
        Assert.assertEquals("fcc1f9fa", hex(h));
    }

    @Test
    public void testHash128() {
        // hash 128 bits (16 bytes)

        String h = HashUtil.hash128("");
        Assert.assertEquals(32, h.length());
        Assert.assertEquals("00000000000000000000000000000000", h);

        h = HashUtil.hash128("q");
        Assert.assertEquals(32, h.length());
        Assert.assertEquals("b1aba139b20c3ebcf667a14f41c7d17c", h);

        h = HashUtil.hash128("qq");
        Assert.assertEquals(32, h.length());
        Assert.assertEquals("2dbabe8ac8d8ce9eedc4b97add0f7c7c", h);

        h = HashUtil.hash128("qwertyuiop[]asdfghjkl;'zxcvbnm,./");
        Assert.assertEquals(32, h.length());
        Assert.assertEquals("49780e7800e613230520ed7b1116fef5", h);
    }

    @Test
    public void testHash128WithBytes() {
        // hash 128 bits (16 bytes)

        byte[] h = HashUtil.hash128(b(""));
        Assert.assertEquals(16, h.length);
        Assert.assertEquals("00000000000000000000000000000000", hex(h));

        h = HashUtil.hash128(b("q"));
        Assert.assertEquals(16, h.length);
        Assert.assertEquals("b1aba139b20c3ebcf667a14f41c7d17c", hex(h));

        h = HashUtil.hash128(b("qq"));
        Assert.assertEquals(16, h.length);
        Assert.assertEquals("2dbabe8ac8d8ce9eedc4b97add0f7c7c", hex(h));

        h = HashUtil.hash128(b("qwertyuiop[]asdfghjkl;'zxcvbnm,./"));
        Assert.assertEquals(16, h.length);
        Assert.assertEquals("49780e7800e613230520ed7b1116fef5", hex(h));
    }

    private static byte[] b(String string) {
        return string.getBytes();
    }

    private static String hex(byte[] bytes) {
        return Bytes.toHex(bytes);
    }
}
