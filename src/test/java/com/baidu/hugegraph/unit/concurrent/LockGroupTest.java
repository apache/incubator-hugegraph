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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;

import com.baidu.hugegraph.concurrent.AtomicLock;
import com.baidu.hugegraph.concurrent.KeyLock;
import com.baidu.hugegraph.concurrent.LockGroup;
import com.baidu.hugegraph.concurrent.RowLock;
import com.baidu.hugegraph.testutil.Assert;

public class LockGroupTest {

    private static final String GROUP = "testGroup";

    private LockGroup group = new LockGroup(GROUP);

    @Test
    public void testLock() {
        Lock lock = this.group.lock("lock");
        Assert.assertTrue(lock instanceof ReentrantLock);
        Lock lock1 = this.group.lock("lock");
        Assert.assertSame(lock, lock1);
    }

    @Test
    public void testAtomicLock() {
        AtomicLock lock = this.group.atomicLock("lock");
        Assert.assertNotNull(lock);
        AtomicLock lock1 = this.group.atomicLock("lock");
        Assert.assertSame(lock, lock1);
    }

    @Test
    public void testReadWriteLock() {
        ReadWriteLock lock = this.group.readWriteLock("lock");
        Assert.assertTrue(lock instanceof ReentrantReadWriteLock);
        ReadWriteLock lock1 = this.group.readWriteLock("lock");
        Assert.assertSame(lock, lock1);
    }

    @Test
    public void testKeyLock() {
        KeyLock lock = this.group.keyLock("lock");
        Assert.assertNotNull(lock);
        KeyLock lock1 = this.group.keyLock("lock");
        Assert.assertSame(lock, lock1);
    }

    @Test
    public void testKeyLockWithSize() {
        KeyLock lock = this.group.keyLock("lock", 10);
        Assert.assertNotNull(lock);
        KeyLock lock1 = this.group.keyLock("lock");
        Assert.assertSame(lock, lock1);
    }

    @Test
    public void testRowLock() {
        RowLock<?> lock = this.group.rowLock("lock");
        Assert.assertNotNull(lock);
        RowLock<?> lock1 = this.group.rowLock("lock");
        Assert.assertSame(lock, lock1);
    }

    @Test
    public void testName() {
        Assert.assertEquals(GROUP, this.group.name());
    }
}
