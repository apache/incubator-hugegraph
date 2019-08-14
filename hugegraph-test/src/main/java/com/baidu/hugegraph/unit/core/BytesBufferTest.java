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

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdGenerator.UuidId;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;

public class BytesBufferTest extends BaseUnitTest {

    @Test
    public void testAllocate() {
        Assert.assertEquals(0, BytesBuffer.allocate(0).array().length);
        Assert.assertEquals(0, BytesBuffer.allocate(0).bytes().length);

        Assert.assertEquals(4, BytesBuffer.allocate(4).array().length);
        Assert.assertEquals(0, BytesBuffer.allocate(4).bytes().length);

        BytesBuffer buffer4 = BytesBuffer.allocate(4);
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0},
                                 buffer4.write(new byte[4]).bytes());

        BytesBuffer buffer2 = BytesBuffer.allocate(2);
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0},
                                 buffer2.write(new byte[4]).bytes());

        BytesBuffer buffer0 = BytesBuffer.allocate(0);
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0},
                                 buffer0.write(new byte[4]).bytes());
    }

    @Test
    public void testWrap() {
        BytesBuffer buffer4 = BytesBuffer.wrap(new byte[]{1, 2, 3, 4});
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, buffer4.array());
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, buffer4.read(4));
    }

    @Test
    public void testStringId() {
        Id id = IdGenerator.of("abc");
        byte[] bytes = new byte[]{-126, 97, 98, 99};

        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(4)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of("abcd");
        bytes = new byte[]{-125, 97, 98, 99, 100};

        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(5)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());
    }

    @Test
    public void testStringIdWithBigSize() {
        Id id = IdGenerator.of(genString(127));
        byte[] bytes = genBytes(128);
        bytes[0] = (byte) 0xfe;

        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(0).writeId(IdGenerator.of(genString(128)));
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(0).writeId(IdGenerator.of(genString(130)));
        });
    }

    @Test
    public void testStringBigId() {
        Id id = IdGenerator.of(genString(128));
        byte[] bytes = genBytes(130);
        bytes[0] = (byte) 0x80;
        bytes[1] = (byte) 0x7f;
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id, true).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId(true));

        id = IdGenerator.of(genString(32512));
        bytes = genBytes(32514);
        bytes[0] = (byte) 0xfe;
        bytes[1] = (byte) 0xff;
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id, true).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId(true));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(0).writeId(IdGenerator.of(genString(32513)),
                                            true);
        });
    }

    @Test
    public void testNumberId() {
        Id id = IdGenerator.of(0);
        byte[] bytes = new byte[]{16, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(1);
        bytes = new byte[]{16, 1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Byte.MAX_VALUE);
        bytes = new byte[]{16, 127};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Byte.MAX_VALUE + 1);
        bytes = new byte[]{48, 0, -128};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(255);
        bytes = new byte[]{48, 0, -1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(256);
        bytes = new byte[]{48, 1, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Short.MAX_VALUE);
        bytes = new byte[]{48, 127, -1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Short.MAX_VALUE + 1);
        bytes = new byte[]{80, 0, 0, -128, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MAX_VALUE);
        bytes = new byte[]{80, 127, -1, -1, -1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MAX_VALUE + 1L);
        bytes = new byte[]{112, 0, 0, 0, -128, 0, 0, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(BytesBuffer.ID_MAX);
        bytes = new byte[]{127, -1, -1, -1, -1, -1, -1, -1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id bigiId = IdGenerator.of(BytesBuffer.ID_MAX + 1L);
            BytesBuffer.allocate(2).writeId(bigiId);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id bigiId = IdGenerator.of(Long.MAX_VALUE);
            BytesBuffer.allocate(2).writeId(bigiId);
        });
    }

    @Test
    public void testNumberIdWithNegValue() {
        Id id = IdGenerator.of(0);
        byte[] bytes = new byte[]{16, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-1);
        bytes = new byte[]{0, -1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Byte.MIN_VALUE);
        bytes = new byte[]{0, -128};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Byte.MIN_VALUE - 1);
        bytes = new byte[]{32, -1, 127};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-255);
        bytes = new byte[]{32, -1, 1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-256);
        bytes = new byte[]{32, -1, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Short.MIN_VALUE);
        bytes = new byte[]{32, -128, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Short.MIN_VALUE - 1);
        bytes = new byte[]{64, -1, -1, 127, -1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MIN_VALUE);
        bytes = new byte[]{64, -128, 0, 0, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MIN_VALUE - 1L);
        bytes = new byte[]{111, -1, -1, -1, 127, -1, -1, -1};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(BytesBuffer.ID_MIN);
        bytes = new byte[]{96, 0, 0, 0, 0, 0, 0, 0};
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id bigiId = IdGenerator.of(BytesBuffer.ID_MIN - 1L);
            BytesBuffer.allocate(2).writeId(bigiId);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id bigiId = IdGenerator.of(Long.MIN_VALUE);
            BytesBuffer.allocate(2).writeId(bigiId);
        });
    }

    @Test
    public void testUuidId() {
        Id id = IdGenerator.of("835e1153928149578691cf79258e90eb", true);
        byte[] bytes = genBytes("ff835e1153928149578691cf79258e90eb");

        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(17)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = new UuidId(UUID.randomUUID());
        bytes = BytesBuffer.allocate(0).writeId(id).bytes();
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());
    }

    @Test
    public void testVarInt() {
        Assert.assertArrayEquals(new byte[]{0},
                                 BytesBuffer.allocate(5).writeVInt(0).bytes());
        Assert.assertArrayEquals(new byte[]{1},
                                 BytesBuffer.allocate(5).writeVInt(1).bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(128)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0x7f},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(16383)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(16384)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(16385)
                                            .bytes());

        Assert.assertArrayEquals(new byte[]{-113, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5).writeVInt(-1).bytes());
        Assert.assertArrayEquals(new byte[]{-121, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(Integer.MAX_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-120, -128, -128, -128, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(Integer.MIN_VALUE)
                                            .bytes());

        for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
            BytesBuffer buffer = BytesBuffer.allocate(5).writeVInt(i);
            Assert.assertEquals(i, buffer.flip().readVInt());
        }

        Random random = new Random();
        for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE;) {
            BytesBuffer buffer = BytesBuffer.allocate(5).writeVInt(i);
            Assert.assertEquals(i, buffer.flip().readVInt());

            int old = i;
            i += random.nextInt(Short.MAX_VALUE);
            if (old > 0 && i < 0) {
                // overflow
                break;
            }
        }
    }

    @Test
    public void testVarLong() {
        Assert.assertArrayEquals(new byte[]{0},
                                 BytesBuffer.allocate(5).writeVLong(0).bytes());
        Assert.assertArrayEquals(new byte[]{1},
                                 BytesBuffer.allocate(5).writeVLong(1).bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(128)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0x7f},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(16383)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(16384)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(16385)
                                            .bytes());

        Assert.assertArrayEquals(new byte[]{-127, -1, -1, -1, -1,
                                            -1, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5).writeVLong(-1).bytes());
        Assert.assertArrayEquals(new byte[]{-121, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Integer.MAX_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-127, -1, -1, -1, -1,
                                            -8, -128, -128, -128, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Integer.MIN_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-1, -1, -1, -1, -1,
                                            -1, -1, -1, 127},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Long.MAX_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-127, -128, -128, -128, -128,
                                            -128, -128, -128, -128, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Long.MIN_VALUE)
                                            .bytes());

        for (long i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
            BytesBuffer buffer = BytesBuffer.allocate(10).writeVLong(i);
            Assert.assertEquals(i, buffer.flip().readVLong());
        }

        Random random = new Random();
        for (long i = Long.MIN_VALUE; i < Long.MAX_VALUE;) {
            BytesBuffer buffer = BytesBuffer.allocate(10).writeVLong(i);
            Assert.assertEquals(i, buffer.flip().readVLong());

            long old = i;
            i += (random.nextLong() >>> 8);
            if (old > 0 && i < 0) {
                // overflow
                break;
            }
        }
    }

    @Test
    public void testProperty() {

    }

    @Test
    public void testString() {

    }

    private static String genString(int len) {
        return new String(new char[len]).replace("\0", "a");
    }

    private byte[] genBytes(int len) {
        byte[] bytes = new byte[len];
        Arrays.fill(bytes, (byte) 'a');
        return bytes;
    }

    private byte[] genBytes(String string) {
        int size = string.length() / 2;
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            String b = string.substring(i * 2, i * 2 + 2);
            bytes[i] = Integer.valueOf(b, 16).byteValue();
        }
        return bytes;
    }
}
