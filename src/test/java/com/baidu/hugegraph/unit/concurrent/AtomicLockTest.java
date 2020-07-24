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

import org.junit.Test;

import com.baidu.hugegraph.concurrent.AtomicLock;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;

public class AtomicLockTest extends BaseUnitTest {

    @Test
    public void testLockUnlock() {
        AtomicLock lock = new AtomicLock("lock");
        Assert.assertEquals("lock", lock.name());

        Assert.assertTrue(lock.lock(0));
        try {
            Assert.assertFalse(lock.lock(1));
            // lock in other threads
            runWithThreads(2, () -> {
                Assert.assertFalse(lock.tryLock());
            });
            lock.unlock();
        } finally {
            lock.unlock();
            // unlock multi times is OK
            lock.unlock();
            lock.unlock();
        }

        Assert.assertThrows(RuntimeException.class, () -> {
            lock.lock(-1);
        }, e -> {
            Assert.assertContains("Locking retry times should be in [0, 10], " +
                                  "but got -1", e.getMessage());
        });

        Assert.assertThrows(RuntimeException.class, () -> {
            lock.lock(11);
        }, e -> {
            Assert.assertContains("Locking retry times should be in [0, 10], " +
                                  "but got 11", e.getMessage());
        });
    }
}
