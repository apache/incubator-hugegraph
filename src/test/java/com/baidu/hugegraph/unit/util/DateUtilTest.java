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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.DateUtil;

public class DateUtilTest extends BaseUnitTest {

    @Test
    public void testParse() {
        Date date1 = DateUtil.parse("2020-06-12 12:00:00");
        Date date2 = DateUtil.parse("2020-06-13");
        Assert.assertNotEquals(date1, date2);
        Assert.assertTrue(date1.before(date2));

        Date date3 = DateUtil.parse("2020-06-12");
        Date date4 = DateUtil.parse("2020-06-12 00:00:00.00");
        Assert.assertEquals(date3, date4);

        Date date5 = DateUtil.parse("2020-06-12 00:00:00.001");
        Assert.assertNotEquals(date3, date5);
        Assert.assertTrue(date3.before(date5));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DateUtil.parse("2018-");
        }, e -> {
            Assert.assertContains("Expected date format is:", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DateUtil.parse("2018-15-07 12:00:00.f");
        }, e -> {
            Assert.assertContains("Expected date format is:", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DateUtil.parse("2018-15-07 12:00:00");
        }, e -> {
            Assert.assertContains("Value 15 for monthOfYear must be " +
                                  "in the range [1,12]", e.getMessage());
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
    public void testParseCornerDateValue() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        int threadCount = 10;
        List<Thread> threads = new ArrayList<>(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);
        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    Assert.assertEquals(new Date(-62167248343000L),
                                        DateUtil.parse("0", "yyyy"));
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            });
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(0, errorCount.get());
    }

    @Test
    public void testToPattern() {
        Object pattern = DateUtil.toPattern("yyyyMMdd HH:mm:ss.SSS");
        Assert.assertEquals("yyyyMMdd HH:mm:ss.SSS", pattern);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DateUtil.toPattern("iyyyyMMdd");
        }, e -> {
            Assert.assertContains("Illegal pattern component: i", e.getMessage());
        });
    }
}
