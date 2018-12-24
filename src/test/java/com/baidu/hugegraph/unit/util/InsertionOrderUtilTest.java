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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableList;

public class InsertionOrderUtilTest extends BaseUnitTest {

    @Test
    public void testSet() {
        Set<Integer> set = InsertionOrderUtil.newSet();
        set.add(4);
        set.add(2);
        set.add(5);
        set.add(1);
        set.add(3);

        Assert.assertEquals(ImmutableList.of(4, 2, 5, 1, 3),
                            ImmutableList.copyOf(set));
    }

    @Test
    public void testList() {
        List<Integer> list = InsertionOrderUtil.newList();
        list.add(4);
        list.add(2);
        list.add(5);
        list.add(1);
        list.add(3);

        Assert.assertEquals(ImmutableList.of(4, 2, 5, 1, 3),
                            ImmutableList.copyOf(list));
    }

    @Test
    public void testMap() {
        Map<Integer, Integer> map = InsertionOrderUtil.newMap();
        map.put(4, 4);
        map.put(2, 2);
        map.put(5, 5);
        map.put(1, 1);
        map.put(3, 3);

        Assert.assertEquals(ImmutableList.of(4, 2, 5, 1, 3),
                            ImmutableList.copyOf(map.keySet()));
    }
}
