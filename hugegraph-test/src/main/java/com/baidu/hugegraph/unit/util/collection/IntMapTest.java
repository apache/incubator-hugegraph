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

package com.baidu.hugegraph.unit.util.collection;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.BiFunction;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.collection.IntIterator;
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
        testIntMap(map);
    }

    @Test
    public void testIntFixedMapBySegments() {
        IntMap map = fixedBySegments(eachCount, 4);
        testIntMap(map);

        IntMap map2 = fixedBySegments(eachCount, 400);
        testIntMap(map2);

        IntMap map3 = fixedBySegments(eachCount, 4000);
        testIntMap(map3);
    }

    private void testIntMap(IntMap map) {
        Assert.assertEquals(0, map.size());
        Map<Integer, Integer> jucMap = new HashMap<>();

        Assert.assertEquals(0, map.size());
        Assert.assertTrue(map.concurrent());

        int modUpdate = 1 + new Random().nextInt(100);
        Random random = new Random();
        for (int k = 0; k < eachCount; k++) {
            if(k % modUpdate == 0) {
                int v = random.nextInt(eachCount);
                map.put(k, v);
                jucMap.put(k, v);
            }
        }

        Assert.assertEquals(jucMap.size(), map.size());
        for (int k = 0; k < eachCount; k++) {
            if (jucMap.containsKey(k)) {
                Assert.assertTrue("expect " + k, map.containsKey(k));
                Assert.assertEquals((int) jucMap.get(k), map.get(k));
            } else {
                Assert.assertFalse("unexpect " + k, map.containsKey(k));
            }
        }

        for (int k = 0; k < eachCount; k++) {
            int v = random.nextInt(eachCount);
            map.put(k, v);
            jucMap.put(k, v);
        }

        Assert.assertEquals(eachCount, map.size());
        for (int k = 0; k < eachCount; k++) {
            Assert.assertTrue("expect " + k, map.containsKey(k));
            Assert.assertEquals((int) jucMap.get(k), map.get(k));
        }

        int modRemove = 1 + new Random().nextInt(100);
        for (int k = 0; k < eachCount; k++) {
            if(k % modRemove == 0) {
                map.remove(k);
                jucMap.remove(k);
            }
        }

        Assert.assertEquals(jucMap.size(), map.size());
        for (int k = 0; k < eachCount; k++) {
            if (jucMap.containsKey(k)) {
                Assert.assertTrue("expect " + k, map.containsKey(k));
                Assert.assertEquals((int) jucMap.get(k), map.get(k));
            } else {
                Assert.assertFalse("unexpect " + k, map.containsKey(k));
            }
        }

        int outOfBoundKey = eachCount;

        Assert.assertFalse(map.containsKey(outOfBoundKey));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            map.put(outOfBoundKey, 0);
        }, e -> {
            Assert.assertContains("out of bound", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            map.remove(outOfBoundKey);
        }, e -> {
            Assert.assertContains("out of bound", e.getMessage());
        });

        map.clear();
        Assert.assertEquals(0, map.size());
        for (int k = 0; k < eachCount; k++) {
            Assert.assertFalse("unexpect " + k, map.containsKey(k));
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
    public void testIntFixedMapBySegmentsConcurrent() {
        IntMap map = fixedBySegments(eachCount, 4);

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
    public void testIntFixedMapBySegmentsPut() {
        BiFunction<Integer, Integer, Integer> testMap = (capacity, segs) -> {
            IntMap map = fixedBySegments(capacity, segs);

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

            IntIterator values = map.values();
            for (int i = -capacity; i < 0; i++) {
                Assert.assertTrue(values.hasNext());
                Assert.assertEquals(i, values.next());
            }
            for (int i = 0; i < capacity; i++) {
                Assert.assertTrue(values.hasNext());
                Assert.assertEquals(i * i, values.next());
            }

            IntIterator keys = map.keys();
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
    public void testIntFixedMapBySegmentsPutRandom() {
        int count = 100;
        BiFunction<Integer, Integer, Integer> testMap = (capacity, segs) -> {
            IntMap map = fixedBySegments(capacity, segs);

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
        Assert.assertEquals(count, testMap.apply(Integer.MAX_VALUE, 40000));
    }

    @Test
    public void testIntFixedMapBySegmentsKeys() {
        IntMap map = fixedBySegments(Integer.MAX_VALUE, 40);
        map.put(Integer.MAX_VALUE - 1, 1);

        Assert.assertEquals(1, IteratorUtils.count(map.keys().asIterator()));

        for (int i = 0; i < 10000; i++) {
            IntIterator iter = map.keys();
            Assert.assertTrue(iter.hasNext());
            Assert.assertEquals(Integer.MAX_VALUE - 1, iter.next());
            Assert.assertFalse(iter.hasNext());

            Assert.assertThrows(NoSuchElementException.class, () -> {
                iter.next();
            }, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    @Test
    public void testIntFixedMapBySegmentsKeysWithMultiSegs() {
        int segments = 400;
        int segmentSize = Integer.MAX_VALUE / segments;
        int step = 50;
        IntMap map = fixedBySegments(Integer.MAX_VALUE, segments);
        for (int k = 0; k < segments; k += step) {
            map.put(segmentSize * k, k);
        }

        Assert.assertEquals(map.size(),
                            IteratorUtils.count(map.keys().asIterator()));

        for (int i = 0; i < 10; i++) {
            IntIterator iter = map.keys();
            for (int k = 0; k < segments; k += step) {
                Assert.assertTrue(iter.hasNext());
                Assert.assertEquals(segmentSize * k, iter.next());
            }

            Assert.assertFalse(iter.hasNext());
            Assert.assertThrows(NoSuchElementException.class, () -> {
                iter.next();
            }, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    @Test
    public void testIntFixedMapBySegmentsValues() {
        IntMap map = fixedBySegments(Integer.MAX_VALUE, 40);
        map.put(Integer.MAX_VALUE - 1, 1);

        Assert.assertEquals(1, IteratorUtils.count(map.values().asIterator()));

        for (int i = 0; i < 10; i++) {
            IntIterator iter = map.values();
            Assert.assertTrue(iter.hasNext());
            Assert.assertEquals(1, iter.next());
            Assert.assertFalse(iter.hasNext());

            Assert.assertThrows(NoSuchElementException.class, () -> {
                iter.next();
            }, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    @Test
    public void testIntFixedMapBySegmentsValuesWithMultiSegs() {
        int segments = 400;
        int segmentSize = Integer.MAX_VALUE / segments;
        int step = 50;
        IntMap map = fixedBySegments(Integer.MAX_VALUE, segments);
        for (int k = 0; k < segments; k += step) {
            map.put(segmentSize * k, k);
        }

        Assert.assertEquals(map.size(),
                            IteratorUtils.count(map.values().asIterator()));

        for (int i = 0; i < 10; i++) {
            IntIterator iter = map.values();
            for (int k = 0; k < segments; k += step) {
                Assert.assertTrue(iter.hasNext());
                Assert.assertEquals(k, iter.next());
            }

            Assert.assertFalse(iter.hasNext());
            Assert.assertThrows(NoSuchElementException.class, () -> {
                iter.next();
            }, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    private IntMap fixed(int capacity) {
        return new IntMap.IntMapByFixedAddr(capacity);
    }

    private IntMap fixedBySegments(int capacity, int segments) {
        return new IntMap.IntMapBySegments(capacity, segments);
    }
}
