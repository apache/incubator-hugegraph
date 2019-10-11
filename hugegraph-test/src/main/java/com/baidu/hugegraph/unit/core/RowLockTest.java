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

package com.baidu.hugegraph.unit.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.LockUtil;

import static com.baidu.hugegraph.unit.core.LocksTableTest.destroyLockGroup;
import static com.baidu.hugegraph.unit.core.LocksTableTest.genLockGroup;

public class RowLockTest extends BaseUnitTest {

    private static String GRAPH = "graph";
    private static String GROUP = "row_lock";

    private static final int THREADS_NUM = 8;

    @BeforeClass
    public static void setup() {
        genLockGroup(GRAPH, GROUP);
    }

    @AfterClass
    public static void teardown() {
        destroyLockGroup(GRAPH, GROUP);
    }

    @Test
    public void testRowLock() {
        LockUtil.lockRow(GRAPH, GROUP, 1);
        LockUtil.unlockRow(GRAPH, GROUP, 1);
    }

    @Test
    public void testRowLockWithMultiThreads() {
        Set<String> names = new HashSet<>(THREADS_NUM);
        List<Integer> rows = new ArrayList<>(5);
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            rows.add(random.nextInt(THREADS_NUM));
        }

        Assert.assertEquals(0, names.size());

        runWithThreads(THREADS_NUM, () -> {
            LockUtil.lockRows(GRAPH, GROUP, new HashSet<>(rows));
            names.add(Thread.currentThread().getName());
            LockUtil.unlockRows(GRAPH, GROUP, new HashSet<>(rows));
        });

        Assert.assertEquals(THREADS_NUM, names.size());
    }

    @Test
    public void testRowLockWithMultiThreadsWithRandomKey() {
        Set<String> names = new HashSet<>(THREADS_NUM);

        Assert.assertEquals(0, names.size());

        runWithThreads(THREADS_NUM, () -> {
            List<Integer> rows = new ArrayList<>(5);
            Random random = new Random();
            for (int i = 0; i < 5; i++) {
                rows.add(random.nextInt(THREADS_NUM));
            }
            LockUtil.lockRows(GRAPH, GROUP, new HashSet<>(rows));
            names.add(Thread.currentThread().getName());
            LockUtil.unlockRows(GRAPH, GROUP, new HashSet<>(rows));
        });

        Assert.assertEquals(THREADS_NUM, names.size());
    }
}
