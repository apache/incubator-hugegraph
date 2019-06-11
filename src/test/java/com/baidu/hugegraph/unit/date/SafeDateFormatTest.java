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

package com.baidu.hugegraph.unit.date;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.baidu.hugegraph.date.SafeDateFormat;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;

public class SafeDateFormatTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testSafeDateFormatInConcurrency() throws Exception {
        DateFormat format = new SafeDateFormat("yyyy-MM-dd");
        List<String> sources = ImmutableList.of(
                "2010-01-01",
                "2011-02-02",
                "2012-03-03",
                "2013-04-04",
                "2014-05-05",
                "2015-06-06",
                "2016-07-07",
                "2017-08-08",
                "2018-09-09",
                "2019-10-10"
        );
        List<Date> dates = new ArrayList<>(sources.size());

        for (int i = 0; i < sources.size(); i++) {
            Date date = format.parse(sources.get(i));
            Assert.assertEquals(2010 + i, 1900 + date.getYear());
            Assert.assertEquals(i, date.getMonth());
            Assert.assertEquals(1 + i, date.getDate());
            dates.add(date);
        }

        List<Exception> exceptions = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        int threadCount = 10;
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                for (int i = 0; i < sources.size(); i++) {
                    try {
                        Assert.assertEquals(dates.get(i),
                                            format.parse(sources.get(i)));
                        Assert.assertEquals(sources.get(i),
                                            format.format(dates.get(i)));
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
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

        Assert.assertTrue(exceptions.isEmpty());
    }
}
