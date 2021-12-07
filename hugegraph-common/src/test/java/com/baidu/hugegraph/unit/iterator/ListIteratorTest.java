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

package com.baidu.hugegraph.unit.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import com.baidu.hugegraph.iterator.ListIterator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.unit.iterator.ExtendableIteratorTest.CloseableItor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

@SuppressWarnings("resource")
public class ListIteratorTest extends BaseUnitTest {

    private static final Iterator<Integer> EMPTY = Collections.emptyIterator();

    private static final List<Integer> DATA1 = ImmutableList.of(1);
    private static final List<Integer> DATA2 = ImmutableList.of(2, 3);
    private static final List<Integer> DATA3 = ImmutableList.of(4, 5, 6);

    @Test
    public void testCapacity() {
        Iterator<Integer> results;

        results = new ListIterator<>(0, EMPTY);
        Assert.assertFalse(results.hasNext());
        Assert.assertEquals(0, Iterators.size(results));

        results = new ListIterator<>(2, DATA1.iterator());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(1, Iterators.size(results));

        results = new ListIterator<>(2, DATA2.iterator());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(2, Iterators.size(results));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new ListIterator<>(0, DATA1.iterator());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new ListIterator<>(2, DATA3.iterator());
        });
    }

    @Test
    public void testList() {
        ListIterator<Integer> results;

        results = new ListIterator<>(-1, EMPTY);
        Assert.assertEquals(ImmutableList.of(), results.list());

        results = new ListIterator<>(-1, DATA1.iterator());
        Assert.assertEquals(ImmutableList.of(1), results.list());

        results = new ListIterator<>(-1, DATA2.iterator());
        Assert.assertEquals(ImmutableList.of(2, 3), results.list());

        results = new ListIterator<>(-1, DATA3.iterator());
        Assert.assertEquals(ImmutableList.of(4, 5, 6), results.list());
    }

    @Test
    public void testHasNext() {
        Iterator<Integer> origin = DATA1.iterator();
        Assert.assertTrue(origin.hasNext());

        Iterator<Integer> results = new ListIterator<>(-1, origin);
        Assert.assertTrue(results.hasNext());
        Assert.assertTrue(results.hasNext());
        Assert.assertFalse(origin.hasNext());
    }

    @Test
    public void testNext() {
        Iterator<Integer> results = new ListIterator<>(-1, DATA1.iterator());
        Assert.assertEquals(1, (int) results.next());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }

    @Test
    public void testNextAfterList() {
        Iterator<Integer> results = new ListIterator<>(-1, DATA1.iterator());
        Assert.assertEquals(ImmutableList.of(1),
                            ((ListIterator<?>) results).list());
        Assert.assertEquals(1, (int) results.next());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }

    @Test
    public void testNextWithMultiTimes() {
        Iterator<Integer> results = new ListIterator<>(-1, DATA2.iterator());
        Assert.assertEquals(2, (int) results.next());
        Assert.assertEquals(3, (int) results.next());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }

    @Test
    public void testHasNextAndNext() {
        Iterator<Integer> results = new ListIterator<>(-1, DATA1.iterator());
        Assert.assertTrue(results.hasNext());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(1, (int) results.next());
        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results.next();
        });
    }

    @Test
    public void testRemove() {
        List<Integer> list = new ArrayList<>(DATA3);
        ListIterator<Integer> results = new ListIterator<>(-1, list.iterator());

        Assert.assertEquals(ImmutableList.of(4, 5, 6), list);
        Assert.assertEquals(ImmutableList.of(4, 5, 6), results.list());

        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            results.remove();
        });
        results.next();
        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            results.remove();
        });

        Assert.assertEquals(ImmutableList.of(4, 5, 6), list);
        Assert.assertEquals(ImmutableList.of(4, 5, 6), results.list());
    }

    @Test
    public void testRemoveWithoutResult() {
        Iterator<Integer> results = new ListIterator<>(-1, EMPTY);
        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            results.remove();
        });

        List<Integer> list0 = new ArrayList<>();
        Iterator<Integer> results2 = new ListIterator<>(-1, list0.iterator());
        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            results2.remove();
        });
    }

    @Test
    public void testClose() throws Exception {
        CloseableItor<Integer> c1 = new CloseableItor<>(DATA1.iterator());

        ListIterator<Integer> results = new ListIterator<>(-1, c1);

        Assert.assertFalse(c1.closed());

        results.close();

        Assert.assertTrue(c1.closed());
    }

    @Test
    public void testListWithConstructFromList() {
        ListIterator<Integer> results;

        results = new ListIterator<>(ImmutableList.of());
        Assert.assertEquals(ImmutableList.of(), results.list());

        results = new ListIterator<>(DATA1);
        Assert.assertEquals(ImmutableList.of(1), results.list());

        results = new ListIterator<>(DATA2);
        Assert.assertEquals(ImmutableList.of(2, 3), results.list());

        results = new ListIterator<>(DATA3);
        Assert.assertEquals(ImmutableList.of(4, 5, 6), results.list());
    }

    @Test
    public void testHasNextAndNextWithConstructFromList() {
        ListIterator<Integer> results0 = new ListIterator<>(ImmutableList.of());
        Assert.assertFalse(results0.hasNext());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results0.next();
        });

        ListIterator<Integer> results1 = new ListIterator<>(DATA1);
        Assert.assertTrue(results1.hasNext());
        Assert.assertEquals(1, results1.next());
        Assert.assertFalse(results1.hasNext());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results1.next();
        });

        ListIterator<Integer> results3 = new ListIterator<>(DATA3);
        Assert.assertTrue(results3.hasNext());
        Assert.assertEquals(4, results3.next());
        Assert.assertTrue(results3.hasNext());
        Assert.assertEquals(5, results3.next());
        Assert.assertTrue(results3.hasNext());
        Assert.assertEquals(6, results3.next());
        Assert.assertFalse(results3.hasNext());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results3.next();
        });
    }

    @Test
    public void testNextWithConstructFromList() {
        ListIterator<Integer> results0 = new ListIterator<>(ImmutableList.of());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results0.next();
        });

        ListIterator<Integer> results3 = new ListIterator<>(DATA3);
        Assert.assertEquals(4, results3.next());
        Assert.assertEquals(5, results3.next());
        Assert.assertEquals(6, results3.next());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results3.next();
        });
    }

    @Test
    public void testNextAfterListWithConstructFromList() {
        ListIterator<Integer> results3 = new ListIterator<>(DATA3);
        Assert.assertEquals(ImmutableList.of(4, 5, 6), results3.list());
        Assert.assertEquals(4, results3.next());
        Assert.assertEquals(5, results3.next());
        Assert.assertEquals(6, results3.next());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            results3.next();
        });
    }
}
