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

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.backend.id.IdUtil;

public class IdUtilTest {

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
}
