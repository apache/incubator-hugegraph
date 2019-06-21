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

package com.baidu.hugegraph.unit.util;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.util.StringUtil;
import com.google.common.collect.ImmutableList;

public class StringUtilTest {

    @Test
    public void testDesc() {
        Id id1 = IdGenerator.of(1);
        Id id2 = IdGenerator.of("2");
        Assert.assertEquals(".foo(\"1\",\"2\")",
                            StringUtil.desc("foo", ImmutableList.of(id1, id2)));

        List<Id> empty = ImmutableList.of();
        Assert.assertEquals(".\'\'()", StringUtil.desc("\'\'", empty));
        Assert.assertEquals(".\"\"()", StringUtil.desc("\"\"", empty));
        Assert.assertEquals(".1\'2\'3()", StringUtil.desc("1\'2\'3", empty));
        Assert.assertEquals(".a\'b\'c()", StringUtil.desc("a\'b\'c", empty));
    }

    @Test
    public void testEscape() {
        Assert.assertEquals("a2b2c",
                            StringUtil.escape('2', '\u0000', "a", "b", "c"));
        Assert.assertEquals("12\u0000223",
                            StringUtil.escape('2', '\u0000', "1", "2", "3"));
    }

    @Test
    public void testUnescape() {
        Assert.assertArrayEquals(new String[]{"a", "b>c", "d"},
                                 StringUtil.unescape("a>b/>c>d", ">", "/"));
        Assert.assertEquals(1, StringUtil.unescape("", "", "").length);
        Assert.assertEquals(1, StringUtil.unescape("foo", "bar", "baz").length);
    }

    @Test
    public void testValueOf() {
        Assert.assertTrue(StringUtil.valueOf(Integer.class, "0")
                          instanceof Integer);
        Assert.assertTrue(StringUtil.valueOf(Long.class, "0") instanceof Long);

        Assert.assertEquals(12, StringUtil.valueOf(Integer.class, "12"));
        Assert.assertEquals(123L, StringUtil.valueOf(Long.class, "123"));
        Assert.assertEquals(12.34d, StringUtil.valueOf(Double.class, "12.34"));
        Assert.assertEquals(12.34f, StringUtil.valueOf(Float.class, "12.34"));
        Assert.assertEquals((byte) 12, StringUtil.valueOf(Byte.class, "12"));
        Assert.assertEquals((short) 12, StringUtil.valueOf(Short.class, "12"));
    }
}
