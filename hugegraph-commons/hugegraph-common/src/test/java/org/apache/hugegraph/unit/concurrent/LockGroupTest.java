/*
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

package org.apache.hugegraph.unit.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;

import org.apache.hugegraph.concurrent.AtomicLock;
import org.apache.hugegraph.concurrent.KeyLock;
import org.apache.hugegraph.concurrent.LockGroup;
import org.apache.hugegraph.concurrent.RowLock;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;

public class LockGroupTest extends BaseUnitTest {

    private static final String GROUP = "LockGroupTest-test-group";

    private final LockGroup group = new LockGroup(GROUP);

    @Test
    public void testLock() {
        Lock lock = this.group.lock("lock");
        Assert.assertTrue(lock instanceof ReentrantLock);
        Lock lock1 = this.group.lock("lock");
        Assert.assertSame(lock, lock1);

        lock1.lock();
        try {
            // lock again is OK
            lock1.lock();
            // lock in other threads
            runWithThreads(2, () -> {
                Assert.assertFalse(lock1.tryLock());
            });
            lock1.unlock();
        } finally {
            lock1.unlock();
        }
    }

    @Test
    public void testAtomicLock() {
        AtomicLock lock = this.group.atomicLock("lock");
        Assert.assertNotNull(lock);
        AtomicLock lock1 = this.group.atomicLock("lock");
        Assert.assertSame(lock, lock1);
        Assert.assertEquals("lock", lock1.name());
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
