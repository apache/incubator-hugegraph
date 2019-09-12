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

package com.baidu.hugegraph.unit.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.Id.IdType;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.primitives.Bytes;

public class IdTest extends BaseUnitTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testStringId() {
        Id id = IdGenerator.of("test-id");

        Assert.assertEquals(IdType.STRING, id.type());
        Assert.assertTrue(id.string());
        Assert.assertFalse(id.number());
        Assert.assertFalse(id.uuid());

        Assert.assertEquals(7, id.length());

        Assert.assertEquals("test-id", id.asString());
        Assert.assertEquals("test-id", id.toString());
        Assert.assertEquals("test-id", id.asObject());
        Assert.assertArrayEquals(StringEncoding.encode("test-id"),
                                 id.asBytes());

        Assert.assertEquals("test-id".hashCode(), id.hashCode());
        Assert.assertEquals(IdGenerator.of("test-id"), id);
        Assert.assertNotEquals(IdGenerator.of("test-id2"), id);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            id.asLong();
        });

        Assert.assertEquals("test-id", IdGenerator.asStoredString(id));
        Assert.assertEquals(id, IdGenerator.ofStoredString("test-id",
                                                           IdType.STRING));
    }

    @Test
    public void testLongId() {
        Id id = IdGenerator.of(123L);

        Assert.assertEquals(IdType.LONG, id.type());
        Assert.assertFalse(id.string());
        Assert.assertTrue(id.number());
        Assert.assertFalse(id.uuid());

        Assert.assertEquals(8, id.length());

        Assert.assertEquals(123L, id.asLong());
        Assert.assertEquals(123L, id.asObject());
        Assert.assertEquals("123", id.asString());
        Assert.assertEquals("123", id.toString());
        Assert.assertArrayEquals(NumericUtil.longToBytes(123L),
                                 id.asBytes());

        Assert.assertEquals(Long.hashCode(123L), id.hashCode());
        Assert.assertEquals(IdGenerator.of(123L), id);
        Assert.assertEquals(IdGenerator.of(123), id);
        Assert.assertNotEquals(IdGenerator.of(1233), id);
        Assert.assertNotEquals(IdGenerator.of("123"), id);

        Assert.assertEquals("21w", IdGenerator.asStoredString(id));
        Assert.assertEquals(id, IdGenerator.ofStoredString("21w", IdType.LONG));
    }

    @Test
    public void testUuidId() {
        Id id = IdGenerator.of("835e1153-9281-4957-8691-cf79258e90eb", true);

        Assert.assertEquals(IdType.UUID, id.type());
        Assert.assertFalse(id.string());
        Assert.assertFalse(id.number());
        Assert.assertTrue(id.uuid());

        Assert.assertEquals(16, id.length());

        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90eb",
                            id.asObject().toString());
        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90eb",
                            id.asString());
        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90eb",
                            id.toString());

        byte[] h = NumericUtil.longToBytes(
                   Long.parseUnsignedLong("835e115392814957", 16));
        byte[] l = NumericUtil.longToBytes(
                   Long.parseUnsignedLong("8691cf79258e90eb", 16));
        Assert.assertArrayEquals(Bytes.concat(h, l), id.asBytes());

        Id id2 = IdGenerator.of("835e1153928149578691cf79258e90eb", true);
        Id id3 = IdGenerator.of("835e1153928149578691cf79258e90eb", false);
        Id id4 = IdGenerator.of("835e1153928149578691cf79258e90ee", true);
        Assert.assertEquals(id2.hashCode(), id.hashCode());
        Assert.assertEquals(id2, id);
        Assert.assertNotEquals(id3, id);
        Assert.assertNotEquals(id4, id);

        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            id.asLong();
        });

        Assert.assertEquals("g14RU5KBSVeGkc95JY6Q6w==",
                            IdGenerator.asStoredString(id));
        Assert.assertEquals(id, IdGenerator.ofStoredString(
                                "g14RU5KBSVeGkc95JY6Q6w==", IdType.UUID));
    }
}
