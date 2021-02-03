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

package com.baidu.hugegraph.unit.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.concurrent.PausableScheduledThreadPool;
import com.baidu.hugegraph.util.ExecutorUtil;

public class PausableScheduledThreadPoolTest {

    @Test
    public void testscheduleWithFixedDelay() throws InterruptedException {
        PausableScheduledThreadPool executor =
                ExecutorUtil.newPausableScheduledThreadPool("test");
        long period = 500L;
        AtomicInteger counter = new AtomicInteger(0);
        executor.scheduleWithFixedDelay(() -> {
            System.out.println("counter: " + counter.incrementAndGet());
        }, period, period, TimeUnit.MILLISECONDS);

        Thread.sleep((long) (2.1 * period));
        Assert.assertEquals(2, counter.get());

        // pause
        executor.pauseSchedule();
        Thread.sleep(period);
        Assert.assertEquals(2, counter.get());

        // resume
        executor.resumeSchedule();
        Thread.sleep((long) (0.5 * period));
        Assert.assertEquals(3, counter.get());

        Thread.sleep((long) (0.6 * period));
        Assert.assertEquals(4, counter.get());

        // pause again
        executor.pauseSchedule();

        executor.shutdown();
        executor.awaitTermination(3L, TimeUnit.SECONDS);
    }

    @Test
    public void testscheduleWithFixedRate() throws InterruptedException {
        PausableScheduledThreadPool executor =
                ExecutorUtil.newPausableScheduledThreadPool(2, "test");
        long period = 500L;
        AtomicInteger counter = new AtomicInteger(0);
        executor.scheduleAtFixedRate(() -> {
            System.out.println("counter: " + counter.incrementAndGet());
        }, period, period, TimeUnit.MILLISECONDS);

        Thread.sleep((long) (2.1 * period));
        Assert.assertEquals(2, counter.get());

        // pause
        executor.pauseSchedule();
        Thread.sleep(period);
        Assert.assertEquals(2, counter.get());

        // resume
        executor.resumeSchedule();
        Thread.sleep((long) (1.1 * period));
        Assert.assertEquals(4, counter.get());

        // pause again
        executor.pauseSchedule();

        executor.shutdownNow();
        executor.awaitTermination(3L, TimeUnit.SECONDS);
    }
}
