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

import java.util.Date;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.DateUtil;

public class DateUtilTest extends BaseUnitTest {

    @Test
    public void testParse() {
        Date date1 = DateUtil.parse("2020-06-12 12:00:00");
        Date date2 = DateUtil.parse("2020-06-13");
        Assert.assertTrue(date1.before(date2));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DateUtil.parse("2018-");
        }, e -> {
            Assert.assertContains("Expected date format is:",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DateUtil.parse("2018-15-07 12:00:00");
        }, e -> {
            Assert.assertContains(", expect format: ", e.getMessage());
        });
    }

    @Test
    public void testNow() {
        Date date1 = DateUtil.now();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // ignore
        }
        Date date2 = DateUtil.now();
        Assert.assertTrue(date1.before(date2));
    }

    @Test
    public void testToPattern() {
        Object pattern = DateUtil.toPattern("yyyyMMdd HH:mm:ss.SSS");
        Assert.assertEquals("yyyyMMdd HH:mm:ss.SSS", pattern);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DateUtil.toPattern("iyyyyMMdd");
        }, e -> {
            Assert.assertContains("Illegal pattern character 'i'", e.getMessage());
        });
    }
}
