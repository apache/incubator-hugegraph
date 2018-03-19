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

import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@SuppressWarnings("resource")
public class MapperIteratorTest extends BaseUnitTest {

    private static final Map<String, Integer> DATA = ImmutableMap.of(
            "first", 1,
            "second", 2,
            "third", 3,
            "forth", 4
    );

    private static final Function<String, Integer> MAPPER = key -> {
        return DATA.get(key);
    };

    @Test
    public void testMapper() {

        AtomicInteger keysCount = new AtomicInteger(0);

        Iterator<String> keys = DATA.keySet().iterator();

        Function<String, Integer> mapper = key -> {
            keysCount.incrementAndGet();

            return DATA.get(key);
        };

        Iterator<Integer> results = new MapperIterator<>(keys, mapper);

        List<Integer> actual = new ArrayList<>();
        while (results.hasNext()) {
            actual.add(results.next());
        }

        List<Integer> expected = ImmutableList.of(1, 2, 3, 4);
        Assert.assertEquals(4, keysCount.get());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testHasNext() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new MapperIterator<>(keys, MAPPER);
        Assert.assertTrue(results.hasNext());
    }

    @Test
    public void testHasNextAndNextWithMultiTimes() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new MapperIterator<>(keys, MAPPER);

        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(results.hasNext());
        }

        for (int i = 0; i < 4; i++) {
            results.next();
        }

        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());

        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }

    @Test
    public void testNext() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new MapperIterator<>(keys, MAPPER);
        // Call next() without hasNext()
        results.next();
    }

    @Test
    public void testNextWithMultiTimes() {
        Iterator<String> keys = DATA.keySet().iterator();

        Iterator<Integer> results = new MapperIterator<>(keys, MAPPER);
        for (int i = 0; i < 4; i++) {
            results.next();
        }
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }

    @Test
    public void testMapperReturnNullThenHasNext() {
        Iterator<String> keys = ImmutableList.of("fifth").iterator();

        Iterator<Integer> results = new MapperIterator<>(keys, key -> {
            return null;
        });
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testMapperReturnNullThenNext() {
        Iterator<String> keys = ImmutableList.of("fifth").iterator();

        Iterator<Integer> results = new MapperIterator<>(keys, key -> {
            return null;
        });
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }
}
