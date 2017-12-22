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
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableList;

public class FilterIteratorTest extends BaseUnitTest {

    private static final List<Integer> DATA = ImmutableList.of(1, 2, 3, 4);

    @Test
    public void testFilter() {

        AtomicInteger valuesCount = new AtomicInteger(0);

        Iterator<Integer> values = DATA.iterator();

        Function<Integer, Boolean> filter = value -> {
            valuesCount.incrementAndGet();
            return (value % 2 == 0);
        };

        Iterator<Integer> results = new FilterIterator<>(values, filter);

        List<Integer> actual = new ArrayList<>();
        while (results.hasNext()) {
            actual.add(results.next());
        }

        Assert.assertEquals(4, valuesCount.get());
        Assert.assertEquals(ImmutableList.of(2, 4), actual);
    }

    @Test
    public void testHasNext() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new FilterIterator<>(vals, val -> true);
        Assert.assertTrue(results.hasNext());
    }

    @Test
    public void testHasNextWithMultiTimesWithoutAnyResult() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new FilterIterator<>(vals, val -> false);
        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testHasNextAndNextWithMultiTimes() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new FilterIterator<>(vals, val -> true);

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
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new FilterIterator<>(vals, val -> true);
        // Call next() without testNext()
        results.next();
    }

    @Test
    public void testNextWithMultiTimes() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new FilterIterator<>(vals, val -> true);
        for (int i = 0; i < 4; i++) {
            results.next();
        }
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }

    @Test
    public void testNextWithMultiTimesWithoutAnyResult() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new FilterIterator<>(vals, val -> false);
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }
}
