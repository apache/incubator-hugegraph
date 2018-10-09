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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.CollectionUtil;

public class CollectionUtilTest {

    @Test
    public void testUnion() {
        List<Integer> first = new ArrayList<>();
        first.add(1);
        first.add(2);

        Set<Integer> second = new HashSet<>();
        second.add(1);
        second.add(3);

        Set<Integer> results = CollectionUtil.union(first, second);
        Assert.assertEquals(3, results.size());
    }

    @Test
    public void testIntersectWithoutModifying() {
        List<Integer> first = new ArrayList<>();
        first.add(1);
        first.add(2);
        first.add(3);

        List<Integer> second = new ArrayList<>();

        second.add(4);
        second.add(5);
        Collection<Integer> results = CollectionUtil.intersect(first, second);
        Assert.assertEquals(0, results.size());
        Assert.assertEquals(3, first.size());

        second.add(3);
        results = CollectionUtil.intersect(first, second);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(3, first.size());

        second.add(1);
        second.add(2);
        results = CollectionUtil.intersect(first, second);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(3, first.size());
    }

    @Test
    public void testIntersectWithModifying() {
        Set<Integer> first = new HashSet<>();
        first.add(1);
        first.add(2);
        first.add(3);

        Set<Integer> second = new HashSet<>();
        second.add(1);
        second.add(2);
        second.add(3);

        Collection<Integer> results = CollectionUtil.intersectWithModify(
                                                     first, second);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(3, first.size());

        // The second set has "1", "2"
        second.remove(3);
        results = CollectionUtil.intersectWithModify(first, second);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(2, first.size());

        // The second set is empty
        second.remove(1);
        second.remove(2);
        results = CollectionUtil.intersectWithModify(first, second);
        Assert.assertEquals(0, results.size());
        Assert.assertEquals(0, first.size());
    }

    @Test
    public void testHasIntersectionBetweenListAndSet() {
        List<Integer> first = new ArrayList<>();
        first.add(1);
        first.add(2);
        first.add(3);

        Set<Integer> second = new HashSet<>();
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(4);
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(1);
        Assert.assertTrue(CollectionUtil.hasIntersection(first, second));
    }

    @Test
    public void testHasIntersectionBetweenSetAndSet() {
        Set<Integer> first = new HashSet<>();
        first.add(1);
        first.add(2);
        first.add(3);

        Set<Integer> second = new HashSet<>();
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(4);
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(1);
        Assert.assertTrue(CollectionUtil.hasIntersection(first, second));
    }
}
