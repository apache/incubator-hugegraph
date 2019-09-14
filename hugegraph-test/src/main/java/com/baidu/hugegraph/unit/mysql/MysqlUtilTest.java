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

package com.baidu.hugegraph.unit.mysql;

import org.junit.Test;

import com.baidu.hugegraph.backend.store.mysql.MysqlUtil;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;

public class MysqlUtilTest extends BaseUnitTest {

    @Test
    public void testEscapeString() {
        Assert.assertEquals("abc", MysqlUtil.escapeString("abc"));
        Assert.assertEquals("abc\"", MysqlUtil.escapeString("abc\""));

        Assert.assertEquals("can\\'t", MysqlUtil.escapeString("can't"));
        Assert.assertEquals("abc\\n", MysqlUtil.escapeString("abc\n"));
        Assert.assertEquals("abc\\r", MysqlUtil.escapeString("abc\r"));
        Assert.assertEquals("abc\\\\", MysqlUtil.escapeString("abc\\"));
        Assert.assertEquals("abc\\0", MysqlUtil.escapeString("abc\u0000"));
        Assert.assertEquals("abc\\Z", MysqlUtil.escapeString("abc\u001a"));
    }

    @Test
    public void testEscapeAndWrapString() {
        Assert.assertEquals("'abc'", MysqlUtil.escapeAndWrapString("abc"));
        Assert.assertEquals("'abc\"'", MysqlUtil.escapeAndWrapString("abc\""));
        Assert.assertEquals("''", MysqlUtil.escapeAndWrapString(""));

        Assert.assertEquals("'can\\'t'",
                            MysqlUtil.escapeAndWrapString("can't"));
        Assert.assertEquals("'abc\\n'",
                            MysqlUtil.escapeAndWrapString("abc\n"));
        Assert.assertEquals("'abc\\r'",
                            MysqlUtil.escapeAndWrapString("abc\r"));
        Assert.assertEquals("'abc\\\\'",
                            MysqlUtil.escapeAndWrapString("abc\\"));
        Assert.assertEquals("'abc\\0'",
                            MysqlUtil.escapeAndWrapString("abc\u0000"));
        Assert.assertEquals("'abc\\Z'",
                            MysqlUtil.escapeAndWrapString("abc\u001a"));
    }
}
