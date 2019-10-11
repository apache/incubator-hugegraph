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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.concurrent.RowLock;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableSet;

public class RowLockTest extends BaseUnitTest {

    private static final int THREADS_NUM = 8;

    @Test
    public void testRowLock() {
        RowLock<Integer> lock = new RowLock<>();
        // Regular lock and unlock
        lock.lock(1);
        lock.unlock(1);

        // Lock one lock multiple times
        lock.lock(1);
        lock.lock(1);
        lock.unlock(1);
        lock.unlock(1);

        // Unlock one lock multiple times
        lock.lock(1);
        lock.unlock(1);
        lock.unlock(1);
    }

    @Test
    public void testRowLockMultiRows() {
        RowLock<Integer> lock = new RowLock<>();
        lock.lockAll(ImmutableSet.of(1, 2, 3));
        lock.unlockAll(ImmutableSet.of(1, 2, 3));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRowLockWithMultiThreads() {
        RowLock lock = new RowLock();
        Set<String> names = new HashSet<>(THREADS_NUM);
        List<Integer> keys = new ArrayList<>(5);
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            keys.add(random.nextInt(THREADS_NUM));
        }

        Assert.assertEquals(0, names.size());

        runWithThreads(THREADS_NUM, () -> {
            lock.lockAll(new HashSet<>(keys));
            names.add(Thread.currentThread().getName());
            lock.unlockAll(new HashSet<>(keys));
        });

        Assert.assertEquals(THREADS_NUM, names.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRowLockWithMultiThreadsWithRandomKey() {
        RowLock lock = new RowLock();
        Set<String> names = new HashSet<>(THREADS_NUM);

        Assert.assertEquals(0, names.size());

        runWithThreads(THREADS_NUM, () -> {
            List<Integer> keys = new ArrayList<>(5);
            Random random = new Random();
            for (int i = 0; i < 5; i++) {
                keys.add(random.nextInt(THREADS_NUM));
            }
            lock.lockAll(new HashSet<>(keys));
            names.add(Thread.currentThread().getName());
            lock.unlockAll(new HashSet<>(keys));
        });

        Assert.assertEquals(THREADS_NUM, names.size());
    }
}
