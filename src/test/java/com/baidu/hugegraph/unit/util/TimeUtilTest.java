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
import com.baidu.hugegraph.util.TimeUtil;

public class TimeUtilTest extends BaseUnitTest {

    @Test
    public void testTimeGen() {
        long time = TimeUtil.timeGen();
        long base = TimeUtil.BASE_TIME;
        long difference = time - base - System.currentTimeMillis();
        Assert.assertTrue(difference < 1000);
    }

    @Test
    public void testTimeGenWithDate() {
        @SuppressWarnings("deprecation")
        Date date = new Date(2019 - 1900, 2, 28);
        long time = TimeUtil.timeGen(date);
        Assert.assertEquals(41904000000L, time);
    }

    @Test
    public void testTimeGenWithLong() {
        long date = TimeUtil.BASE_TIME + 123L;
        long time = TimeUtil.timeGen(date);
        Assert.assertEquals(123L, time);
    }

    @Test
    public void testTillNextMillis() {
        for (int i = 0; i < 100; i++) {
            long lastTimestamp = TimeUtil.timeGen();
            long time = TimeUtil.tillNextMillis(lastTimestamp);
            Assert.assertNotEquals(lastTimestamp, time);
        }
    }
}
