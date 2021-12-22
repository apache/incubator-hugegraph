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

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.collection.IntSet;

public class IntSetTest extends BaseUnitTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    static final int THREADS_NUM = 4;
    static final int batchCount = 2000;
    static final int eachCount = 10000;

    @Test
    public void testIntFixedSet() {
        IntSet set = fixed(eachCount);
        int mod = new Random().nextInt(100);
        for (int i = 0; i < batchCount; i++) {
            for (int k = 0; k < eachCount; k++) {
                set.contains(k);
                if(k % mod == 0) {
                    set.add(k);
                }
            }
        }

        int size = eachCount / mod + 1;
        if (eachCount % mod == 0) {
            size += 1;
        }
        Assert.assertEquals(size, set.size());

        for (int k = 0; k < eachCount; k++) {
            boolean exist = set.contains(k);
            if(k % mod == 0) {
                Assert.assertTrue("expect " + k, exist);
            } else {
                Assert.assertFalse("unexpect " + k, exist);
            }
        }

        int count = set.size();
        for (int k = 0; k < eachCount; k++) {
            boolean exist = set.contains(k);
            if(k % mod == 0) {
                Assert.assertTrue("expect " + k, exist);

                Assert.assertFalse(set.add(k));
                Assert.assertTrue(set.remove(k));
                Assert.assertFalse("unexpect " + k,  set.contains(k));
                Assert.assertEquals(--count, set.size());
            } else {
                Assert.assertFalse("unexpect " + k, exist);

                Assert.assertFalse(set.remove(k));
                Assert.assertTrue(set.add(k));
                Assert.assertTrue("expect " + k,  set.contains(k));
                Assert.assertEquals(++count, set.size());
            }
        }

        int outOfBoundKey = eachCount;

        Assert.assertFalse(set.contains(outOfBoundKey));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            set.add(outOfBoundKey);
        }, e -> {
            Assert.assertContains("out of bound", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            set.remove(outOfBoundKey);
        }, e -> {
            Assert.assertContains("out of bound", e.getMessage());
        });
    }

    @Test
    public void testInttFixedSeConcurrent() {
        IntSet set = fixed(eachCount);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < batchCount; i++) {
                for (int k = 0; k < eachCount; k++) {
                    set.contains(k);
                    set.add(k);
                    set.size();
                }
                set.contains(i);
                Assert.assertEquals(eachCount, set.size());
            }
        });

        Assert.assertEquals(eachCount, set.size());
        for (int k = 0; k < eachCount; k++) {
            Assert.assertTrue("expect " + k, set.contains(k));
        }
    }

    private IntSet fixed(int size) {
        return new IntSet.IntSetByFixedAddr(size);
    }
}
