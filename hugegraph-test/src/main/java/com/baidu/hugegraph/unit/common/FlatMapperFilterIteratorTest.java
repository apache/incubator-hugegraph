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

package com.baidu.hugegraph.unit.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import com.baidu.hugegraph.iterator.FlatMapperFilterIterator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class FlatMapperFilterIteratorTest extends BaseUnitTest {

    private static final Map<String, List<Integer>> DATA = ImmutableMap.of(
            "first", ImmutableList.of(11),
            "second", ImmutableList.of(21, 22),
            "third", ImmutableList.of(31, 32, 33),
            "forth", ImmutableList.of(41, 42, 43, 44)
    );

    @Test
    public void testMapperFilter() {

        AtomicInteger keysCount = new AtomicInteger(0);
        AtomicInteger valuesCount = new AtomicInteger(0);

        Iterator<String> keys = DATA.keySet().iterator();

        Function<String, Iterator<Integer>> mapper = key -> {
            keysCount.incrementAndGet();

            return DATA.get(key).iterator();
        };

        Function<Integer, Boolean> filter = value -> {
            valuesCount.incrementAndGet();

            double f = value / 11F;
            return (f == (int) f);
        };

        Iterator<Integer> results = new FlatMapperFilterIterator<>(keys,
                                                                   mapper,
                                                                   filter);

        List<Integer> actual = new ArrayList<>();
        while (results.hasNext()) {
            actual.add(results.next());
        }

        Assert.assertEquals(4, keysCount.get());
        Assert.assertEquals(10, valuesCount.get());
        Assert.assertEquals(ImmutableList.of(11, 22, 33, 44), actual);
    }

    @Test
    public void testHasNext() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new FlatMapperFilterIterator<>(keys,
                                        key -> DATA.get(key).iterator(),
                                        val -> true);
        Assert.assertTrue(results.hasNext());

        Iterator<Integer> results2 = new FlatMapperFilterIterator<>(keys,
                                         key -> DATA.get(key).iterator(),
                                         val -> false);
        Assert.assertFalse(results2.hasNext());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results2.next();
        });
    }

    @Test
    public void testHasNextWithMultiTimes() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new FlatMapperFilterIterator<>(keys,
                                        key -> DATA.get(key).iterator(),
                                        val -> true);
        for (int i = 0; i < 12; i++) {
            Assert.assertTrue(results.hasNext());
        }
        for (int i = 0; i < 10; i++) {
            results.next();
        }
        Assert.assertFalse(results.hasNext());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });

        Iterator<Integer> results2 = new FlatMapperFilterIterator<>(keys,
                                         key -> DATA.get(key).iterator(),
                                         val -> false);
        Assert.assertFalse(results2.hasNext());
        Assert.assertFalse(results2.hasNext());
    }

    @Test
    public void testNext() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new FlatMapperFilterIterator<>(keys,
                                        key -> DATA.get(key).iterator(),
                                        val -> true);
        // Call next() without hasNext()
        results.next();

        Iterator<Integer> results2 = new FlatMapperFilterIterator<>(keys,
                                         key -> DATA.get(key).iterator(),
                                         val -> false);
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results2.next();
        });
    }

    @Test
    public void testNextWithMultiTimes() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new FlatMapperFilterIterator<>(keys,
                                        key -> DATA.get(key).iterator(),
                                        val -> true);
        for (int i = 0; i < 10; i++) {
            results.next();
        }
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });

        Iterator<Integer> results2 = new FlatMapperFilterIterator<>(keys,
                                         key -> DATA.get(key).iterator(),
                                         val -> false);
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results2.next();
        });
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results2.next();
        });
    }
}
