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

import java.util.Map;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.OrderLimitMap;
import com.google.common.collect.ImmutableList;

public class OrderLimitMapTest {

    @Test
    public void testMap() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(4, 0.4);
        map.put(2, 0.2);
        map.put(5, 0.5);
        map.put(1, 0.2);
        map.put(3, 0.3);

        Assert.assertEquals(5, map.size());

        Assert.assertEquals(0.2, map.get(2), 1E-9);
        Assert.assertEquals(0.4, map.get(4), 1E-9);

        Assert.assertTrue(map.containsKey(1));
        Assert.assertTrue(map.containsKey(3));
        Assert.assertFalse(map.containsKey(6));

        Assert.assertNull(map.get(6));

        Assert.assertEquals(0.5, map.getOrDefault(5, 0.0), 1E-9);
        Assert.assertEquals(0.0, map.getOrDefault(7, 0.0), 1E-9);
    }

    @Test
    public void testOrder() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(1, 0.1);
        map.put(2, 0.2);
        map.put(3, 0.3);
        map.put(4, 0.4);
        map.put(5, 0.5);

        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 2, 1),
                            ImmutableList.copyOf(map.keySet()));
    }

    @Test
    public void testOrderWithIncrOrder() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5, true);
        map.put(1, 0.5);
        map.put(2, 0.4);
        map.put(3, 0.3);
        map.put(4, 0.2);
        map.put(5, 0.1);

        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 2, 1),
                            ImmutableList.copyOf(map.keySet()));
    }

    @Test
    public void testOrderWithDupValue() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(1, 0.1);
        map.put(2, 0.2);
        map.put(3, 0.3);
        map.put(4, 0.2);
        map.put(5, 0.3);

        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(3, 5, 2, 4, 1),
                            ImmutableList.copyOf(map.keySet()));
    }

    @Test
    public void testOrderWithDupValueAndKeyIncrOrder() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(4, 0.2);
        map.put(2, 0.2);
        map.put(1, 0.1);
        map.put(5, 0.3);
        map.put(3, 0.3);

        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(3, 5, 2, 4, 1),
                            ImmutableList.copyOf(map.keySet()));
    }

    @Test
    public void testOrderWithDupKey() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(1, 0.1);
        map.put(2, 0.2);
        map.put(3, 0.3);
        map.put(2, 0.4);
        map.put(3, 0.2);

        Assert.assertEquals(3, map.size());
        Assert.assertEquals(ImmutableList.of(2, 3, 1),
                            ImmutableList.copyOf(map.keySet()));
    }

    @Test
    public void testLimit() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(1, 0.1);
        map.put(2, 0.2);
        map.put(3, 0.3);
        map.put(4, 0.4);
        map.put(5, 0.5);

        map.put(6, 0.6);
        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(6, 5, 4, 3, 2),
                            ImmutableList.copyOf(map.keySet()));

        map.put(7, 0.7);
        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(7, 6, 5, 4, 3),
                            ImmutableList.copyOf(map.keySet()));
    }

    @Test
    public void testLimitWithDupValue() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(1, 0.1);
        map.put(2, 0.2);
        map.put(3, 0.3);
        map.put(4, 0.4);
        map.put(5, 0.5);

        map.put(6, 0.1);
        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 2, 6),
                            ImmutableList.copyOf(map.keySet()));

        map.put(7, 0.3);
        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 7, 2),
                            ImmutableList.copyOf(map.keySet()));

        map.put(8, 0.5);
        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(5, 8, 4, 3, 7),
                            ImmutableList.copyOf(map.keySet()));

        map.put(0, 0.5);
        Assert.assertEquals(5, map.size());
        Assert.assertEquals(ImmutableList.of(0, 5, 8, 4, 3),
                            ImmutableList.copyOf(map.keySet()));
    }

    @Test
    public void testTopN() {
        OrderLimitMap<Integer, Double> map = new OrderLimitMap<>(5);
        map.put(1, 0.1);
        map.put(2, 0.2);
        map.put(3, 0.3);
        map.put(4, 0.4);
        map.put(5, 0.5);

        Map<Integer, Double> top = map.topN(1);
        Assert.assertEquals(ImmutableList.of(5),
                            ImmutableList.copyOf(top.keySet()));

        top = map.topN(3);
        Assert.assertEquals(ImmutableList.of(5, 4, 3),
                            ImmutableList.copyOf(top.keySet()));

        top = map.topN(5);
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 2, 1),
                            ImmutableList.copyOf(top.keySet()));

        top = map.topN(6);
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 2, 1),
                            ImmutableList.copyOf(top.keySet()));
    }
}
