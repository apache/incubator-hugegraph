/*
 * Copyright 2019 HugeGraph Authors
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

package com.baidu.hugegraph.unit.id;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;

public class IdUtilTest {

    @Test
    public void testWriteReadString() {
        Id id = IdGenerator.of(123);
        Assert.assertEquals("L123", IdUtil.writeString(id));
        Assert.assertEquals(id, IdUtil.readString("L123"));

        id = IdGenerator.of("123");
        Assert.assertEquals("S123", IdUtil.writeString(id));
        Assert.assertEquals(id, IdUtil.readString("S123"));

        String uuid = "835e1153-9281-4957-8691-cf79258e90eb";
        id = IdGenerator.of(uuid, true);
        Assert.assertEquals("U" + uuid, IdUtil.writeString(id));
        Assert.assertEquals(id, IdUtil.readString("U" + uuid));

        id = EdgeId.parse("S1>2>3>L4");
        Assert.assertEquals("ES1>2>3>L4", IdUtil.writeString(id));
        Assert.assertEquals(id, IdUtil.readString("ES1>2>3>L4"));

        id = EdgeId.parse("S1111>2222>3>L4444");
        Assert.assertEquals("ES1111>2222>3>L4444", IdUtil.writeString(id));
        Assert.assertEquals(id, IdUtil.readString("ES1111>2222>3>L4444"));
    }

    @Test
    public void testWriteReadBinString() {
        Id id = IdGenerator.of(123);
        ByteBuffer bytes = ByteBuffer.wrap(genBytes("087b"));
        Assert.assertEquals(bytes, IdUtil.writeBinString(id));
        Assert.assertEquals(id, IdUtil.readBinString(bytes));

        id = IdGenerator.of("123");
        bytes = ByteBuffer.wrap(genBytes("82313233"));
        Assert.assertEquals(bytes, IdUtil.writeBinString(id));
        Assert.assertEquals(id, IdUtil.readBinString(bytes));

        String uuid = "835e1153-9281-4957-8691-cf79258e90eb";
        id = IdGenerator.of(uuid, true);
        bytes = ByteBuffer.wrap(genBytes("7f835e1153928149578691cf79258e90eb"));
        Assert.assertEquals(bytes, IdUtil.writeBinString(id));
        Assert.assertEquals(id, IdUtil.readBinString(bytes));

        id = EdgeId.parse("S1>2>3>L4");
        bytes = ByteBuffer.wrap(genBytes("7e803182080233000804"));
        Assert.assertEquals(bytes, IdUtil.writeBinString(id));
        Assert.assertEquals(id, IdUtil.readBinString(bytes));

        id = EdgeId.parse("S1111>2222>3>L4444");
        bytes = ByteBuffer.wrap(genBytes("7e8331313131821808ae330018115c"));
        Assert.assertEquals(bytes, IdUtil.writeBinString(id));
        Assert.assertEquals(id, IdUtil.readBinString(bytes));

        id = EdgeId.parse("L11111111>2222>3>L44444444");
        bytes = ByteBuffer.wrap(genBytes("7e28a98ac7821808ae33002aa62b1c"));
        Assert.assertEquals(bytes, IdUtil.writeBinString(id));
        Assert.assertEquals(id, IdUtil.readBinString(bytes));

        id = EdgeId.parse("L-1111>2222>33>L4444");
        bytes = ByteBuffer.wrap(genBytes("7e03a9821808ae33330018115c"));
        Assert.assertEquals(bytes, IdUtil.writeBinString(id));
        Assert.assertEquals(id, IdUtil.readBinString(bytes));
    }

    @Test
    public void testWriteReadStoredString() {
        Id id = IdGenerator.of(123);
        Assert.assertEquals("L1w", IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString("L1w"));

        id = IdGenerator.of("123");
        Assert.assertEquals("S123", IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString("S123"));

        id = IdGenerator.of("835e1153-9281-4957-8691-cf79258e90eb", true);
        String uuid = "Ug14RU5KBSVeGkc95JY6Q6w==";
        Assert.assertEquals(uuid, IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString(uuid));

        id = EdgeId.parse("S1>2>3>L4");
        Assert.assertEquals("ES1>2>3>L4", IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString("ES1>2>3>L4"));

        id = EdgeId.parse("S1111>2222>3>L4444");
        Assert.assertEquals("ES1111>Yj>3>L15S", IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString("ES1111>Yj>3>L15S"));

        id = EdgeId.parse("L1111>2222>3>L4444");
        Assert.assertEquals("ELHN>Yj>3>L15S", IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString("ELHN>Yj>3>L15S"));

        id = EdgeId.parse("L11111111>2222>3>L44444444");
        String eid = "ELfOg7>Yj>3>L2eYhS";
        Assert.assertEquals(eid, IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString(eid));

        id = EdgeId.parse("L-1111>2222>33>L4444");
        eid = "EL-HN>Yj>33>L15S";
        Assert.assertEquals(eid, IdUtil.writeStoredString(id));
        Assert.assertEquals(id, IdUtil.readStoredString(eid));
    }

    @Test
    public void testEscape() {
        Assert.assertEquals("a2b2c",
                            IdUtil.escape('2', '\u0000', "a", "b", "c"));
        Assert.assertEquals("12\u0000223",
                            IdUtil.escape('2', '\u0000', "1", "2", "3"));
    }

    @Test
    public void testUnescape() {
        Assert.assertArrayEquals(new String[]{"a", "b>c", "d"},
                                 IdUtil.unescape("a>b/>c>d", ">", "/"));
        Assert.assertEquals(1, IdUtil.unescape("", "", "").length);
        Assert.assertEquals(1, IdUtil.unescape("foo", "bar", "baz").length);
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
