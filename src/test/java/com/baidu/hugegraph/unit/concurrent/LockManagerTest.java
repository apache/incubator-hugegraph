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

import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.concurrent.LockGroup;
import com.baidu.hugegraph.concurrent.LockManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;

public class LockManagerTest extends BaseUnitTest {

    private static final String GROUP = "LockManagerTest-test-group";
    private static final String GROUP2 = GROUP + 2;

    @After
    public void teardown() {
        LockManager manager = LockManager.instance();

        if (manager.exists(GROUP)) {
            manager.destroy(GROUP);
        }

        if (manager.exists(GROUP2)) {
            manager.destroy(GROUP2);
        }
    }

    @Test
    public void testCreate() {
        LockManager manager = LockManager.instance();

        LockGroup lockGroup = manager.create(GROUP);
        Assert.assertNotNull(lockGroup);
        Assert.assertEquals(GROUP, lockGroup.name());
        Assert.assertTrue(manager.exists(GROUP));

        Assert.assertFalse(manager.exists(GROUP2));
        LockGroup lockGroup2 = manager.create(GROUP2);
        Assert.assertNotNull(lockGroup2);
        Assert.assertEquals(GROUP2, lockGroup2.name());
        Assert.assertTrue(manager.exists(GROUP2));

        Assert.assertThrows(RuntimeException.class, () -> {
            manager.create(GROUP);
        }, e -> {
            Assert.assertContains("LockGroup 'LockManagerTest-test-group' " +
                                  "already exists", e.getMessage());
        });
    }

    @Test
    public void testGet() {
        LockManager manager = LockManager.instance();

        LockGroup lockGroup = manager.create(GROUP);
        LockGroup lockGroup2 = manager.create(GROUP2);

        Assert.assertSame(lockGroup, manager.get(GROUP));
        Assert.assertSame(lockGroup2, manager.get(GROUP2));
        Assert.assertSame(lockGroup, manager.get(GROUP));
        Assert.assertSame(lockGroup2, manager.get(GROUP2));

        Assert.assertThrows(RuntimeException.class, () -> {
            manager.get("fake-lock-group");
        }, e -> {
            Assert.assertContains("LockGroup 'fake-lock-group' " +
                                  "does not exists", e.getMessage());
        });
    }

    @Test
    public void testDestroy() {
        LockManager manager = LockManager.instance();

        LockGroup lockGroup = manager.create(GROUP);
        LockGroup lockGroup2 = manager.create(GROUP2);

        Assert.assertTrue(manager.exists(GROUP));
        Assert.assertTrue(manager.exists(GROUP2));
        Assert.assertSame(lockGroup, manager.get(GROUP));
        Assert.assertSame(lockGroup2, manager.get(GROUP2));

        manager.destroy(GROUP);
        Assert.assertFalse(manager.exists(GROUP));
        Assert.assertTrue(manager.exists(GROUP2));
        Assert.assertThrows(RuntimeException.class, () -> {
            manager.get(GROUP);
        }, e -> {
            Assert.assertContains("does not exists", e.getMessage());
        });
        Assert.assertSame(lockGroup2, manager.get(GROUP2));

        manager.destroy(GROUP2);
        Assert.assertFalse(manager.exists(GROUP));
        Assert.assertFalse(manager.exists(GROUP2));
        Assert.assertThrows(RuntimeException.class, () -> {
            manager.get(GROUP);
        }, e -> {
            Assert.assertContains("does not exists", e.getMessage());
        });
        Assert.assertThrows(RuntimeException.class, () -> {
            manager.get(GROUP2);
        }, e -> {
            Assert.assertContains("does not exists", e.getMessage());
        });
    }
}
