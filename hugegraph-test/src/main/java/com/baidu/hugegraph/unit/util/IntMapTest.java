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

import java.util.Iterator;
import java.util.Random;
import java.util.function.BiFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.collection.IntMap;

public class IntMapTest extends BaseUnitTest {

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
    public void testIntFixedMap() {
        IntMap map = fixed(eachCount);

        runWithThreads(1, () -> {
            for (int i = 0; i < batchCount; i++) {
                for (int k = 0; k < eachCount; k++) {
                    map.containsKey(k);
                    map.put(k, k);
                }
                map.get(i);
            }
        });

        Assert.assertEquals(eachCount, map.size());
        for (int k = 0; k < eachCount; k++) {
            Assert.assertTrue("expect " + k, map.containsKey(k));
        }
    }

    @Test
    public void testIntFixedMapConcurrent() {
        IntMap map = fixed(eachCount);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < batchCount; i++) {
                for (int k = 0; k < eachCount; k++) {
                    map.containsKey(k);
                    map.put(k, k);
                }
                map.get(i);
            }
        });

        Assert.assertEquals(eachCount, map.size());
        for (int k = 0; k < eachCount; k++) {
            Assert.assertTrue("expect " + k, map.containsKey(k));
        }
    }

    @Test
    public void testIntFixedMapConcurrentSegment() {
        IntMap map = fixed(eachCount, 4);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < batchCount; i++) {
                for (int k = 0; k < eachCount; k++) {
                    map.containsKey(k);
                    map.put(k, k);
                }
                map.get(i);
            }
        });

        Assert.assertEquals(eachCount, map.size());
        for (int k = 0; k < eachCount; k++) {
            Assert.assertTrue("expect " + k, map.containsKey(k));
        }
    }

    @Test
    public void testIntFixedMapSegmentPut() {
        BiFunction<Integer, Integer, Integer> testMap = (capacity, segs) -> {
            IntMap map = fixed(capacity, segs);

            for (int i = 0; i < capacity; i++) {
                Assert.assertTrue(map.put(i, i * i));
                Assert.assertEquals(i * i, map.get(i));
                Assert.assertEquals(true, map.containsKey(i));
                Assert.assertEquals(false, map.containsKey(i + 1));

                int u = -i - 1;
                Assert.assertTrue(map.put(u, u));
                Assert.assertEquals(u, map.get(u));
                Assert.assertEquals(true, map.containsKey(u));
                Assert.assertEquals(false, map.containsKey(u - 1));
            }

            Iterator<Integer> values = map.values();
            for (int i = -capacity; i < 0; i++) {
                Assert.assertTrue(values.hasNext());
                Assert.assertEquals(i, values.next());
            }
            for (int i = 0; i < capacity; i++) {
                Assert.assertTrue(values.hasNext());
                Assert.assertEquals(i * i, values.next());
            }

            Iterator<Integer> keys = map.keys();
            for (int i = -capacity; i < 0; i++) {
                Assert.assertTrue(keys.hasNext());
                Assert.assertEquals(i, keys.next());
            }
            for (int i = 0; i < capacity; i++) {
                Assert.assertTrue(keys.hasNext());
                Assert.assertEquals(i, keys.next());
            }

            Assert.assertThrows(IllegalArgumentException.class, () -> {
                map.put(capacity, 1);
            }, e -> {
                Assert.assertContains("out of bound", e.getMessage());
            });

            Assert.assertThrows(IllegalArgumentException.class, () -> {
                map.put(capacity + 1, 1);
            }, e -> {
                Assert.assertContains("out of bound", e.getMessage());
            });

            return map.size() / 2;
        };

        Assert.assertEquals(10, testMap.apply(10, 1));
        Assert.assertEquals(10, testMap.apply(10, 2));
        Assert.assertEquals(10, testMap.apply(10, 3));
        Assert.assertEquals(10, testMap.apply(10, 4));
        Assert.assertEquals(10, testMap.apply(10, 5));
        Assert.assertEquals(10, testMap.apply(10, 7));
        Assert.assertEquals(10, testMap.apply(10, 9));
        Assert.assertEquals(10, testMap.apply(10, 10));

        Assert.assertEquals(2000, testMap.apply(2000, 1));
        Assert.assertEquals(2000, testMap.apply(2000, 2));
        Assert.assertEquals(2000, testMap.apply(2000, 3));
        Assert.assertEquals(2000, testMap.apply(2000, 100));
        Assert.assertEquals(2000, testMap.apply(2000, 999));
        Assert.assertEquals(2000, testMap.apply(2000, 1000));
        Assert.assertEquals(2000, testMap.apply(2000, 1001));
        Assert.assertEquals(2000, testMap.apply(2000, 1999));
        Assert.assertEquals(2000, testMap.apply(2000, 2000));

//        IntMap map = fixed(Integer.MAX_VALUE, 40);
//        map.put(Integer.MAX_VALUE - 1, 1);
        Assert.assertEquals(10000000, testMap.apply(10000000, 100));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Assert.assertEquals(10, testMap.apply(10, 0));
        }, e -> {
            Assert.assertContains("Invalid segments", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Assert.assertEquals(10, testMap.apply(10, 11));
        }, e -> {
            Assert.assertContains("Invalid capacity", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Assert.assertEquals(2000, testMap.apply(2000, 2001));
        }, e -> {
            Assert.assertContains("Invalid capacity", e.getMessage());
        });
    }

    @Test
    public void testIntFixedMapSegmentPutRandom() {
        int count = 100;
        BiFunction<Integer, Integer, Integer> testMap = (capacity, segs) -> {
            IntMap map = fixed(capacity, segs);

            Random random = new Random();
            for (int i = 0; i < count; i++) {
                int k = random.nextInt(capacity);

                Assert.assertTrue(map.put(k, k * k));
                Assert.assertEquals(k * k, map.get(k));
                Assert.assertEquals(true, map.containsKey(k));

                int u = -k - 1;
                Assert.assertTrue(map.put(u, u));
                Assert.assertEquals(u, map.get(u));
                Assert.assertEquals(true, map.containsKey(u));

                int r = i % 2 == 0 ? k : u;
                Assert.assertEquals(true, map.containsKey(r));
                Assert.assertTrue(map.remove(r));
                Assert.assertEquals(false, map.containsKey(r));
            }

            return map.size();
        };

        Assert.assertEquals(count, testMap.apply(100000000, 100));
        Assert.assertEquals(count, testMap.apply(Integer.MAX_VALUE, 4000));
    }

    private IntMap fixed(int capacity, int segments) {
        return new IntMap.IntMapByBlocks(capacity, segments, size ->
                                           new IntMap.IntMapByFixedAddr(size));
    }

    private IntMap fixed(int capacity) {
        return new IntMap.IntMapByFixedAddr(capacity);
    }
}
