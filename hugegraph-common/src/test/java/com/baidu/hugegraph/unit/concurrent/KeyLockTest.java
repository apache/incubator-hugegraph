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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.junit.Test;

import com.baidu.hugegraph.concurrent.KeyLock;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;

public class KeyLockTest extends BaseUnitTest {

    @Test
    public void testLockUnlock() {
        KeyLock locks = new KeyLock();

        locks.lock("1");
        try {
            // lock again is OK
            locks.lock("1");
            // lock in other threads
            runWithThreads(1, () -> {
                locks.lock("2");
            });
            locks.unlock("1");
        } finally {
            locks.unlock("1");
        }

        Assert.assertThrows(IllegalMonitorStateException.class, () -> {
            locks.unlock("2");
        });

        Assert.assertThrows(IllegalMonitorStateException.class, () -> {
            locks.unlock("3");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.lock(null);
        }, e -> {
            Assert.assertContains("Lock key can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.unlock(null);
        }, e -> {
            Assert.assertContains("Unlock key can't be null", e.getMessage());
        });
    }

    @Test
    public void testLockUnlockAll() {
        KeyLock locks = new KeyLock();

        List<Lock> ls = locks.lockAll("1", 2);
        locks.unlockAll(ls);

        runWithThreads(1, () -> {
            List<Lock> ls2 = locks.lockAll("1", 3);
            locks.unlockAll(ls2);
        });

        List<Lock> ls3 = locks.lockAll("1", 2, 3);
        locks.unlockAll(ls3);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.lockAll("1", null);
        }, e -> {
            Assert.assertContains("Lock key can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.lockAll(null, "1");
        }, e -> {
            Assert.assertContains("Lock key can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.lockAll(Arrays.asList("1", null, 2).toArray());
        }, e -> {
            Assert.assertContains("Lock key can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.lockAll(Arrays.asList().toArray());
        }, e -> {
            Assert.assertContains("Lock keys can't be null or empty",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.lockAll((Object[]) null);
        }, e -> {
            Assert.assertContains("Lock keys can't be null or empty",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            locks.unlockAll(null);
        }, e -> {
            Assert.assertContains("Unlock locks can't be null", e.getMessage());
        });
    }
}
