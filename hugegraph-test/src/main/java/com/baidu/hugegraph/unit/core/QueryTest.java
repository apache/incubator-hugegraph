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

package com.baidu.hugegraph.unit.core;

import org.junit.Test;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.Query.Order;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableMap;

public class QueryTest {

    @Test
    public void testOrderBy() {
        Query query = new Query(HugeType.VERTEX);
        Assert.assertTrue(query.orders().isEmpty());

        query.order(HugeKeys.NAME, Order.ASC);
        Assert.assertEquals(ImmutableMap.of(HugeKeys.NAME, Order.ASC),
                            query.orders());
    }

    @Test
    public void testToString() {
        Query query = new Query(HugeType.VERTEX);
        Assert.assertEquals("Query for VERTEX", query.toString());

        query.page("p1");
        Assert.assertEquals("Query for VERTEX page 'p1'", query.toString());

        query = new Query(HugeType.VERTEX);
        query.limit(10L);
        Assert.assertEquals("Query for VERTEX limit 10", query.toString());

        query = new Query(HugeType.VERTEX);
        query.page("p2");
        query.limit(10L);
        Assert.assertEquals("Query for VERTEX page 'p2', limit 10",
                            query.toString());

        query = new Query(HugeType.VERTEX);
        query.page("p3");
        query.offset(100L);
        query.limit(10L);
        Assert.assertEquals("Query for VERTEX page 'p3', offset 100, limit 10",
                            query.toString());

        query = new Query(HugeType.VERTEX);
        query.page("");
        query.offset(100L);
        query.limit(10L);
        query.order(HugeKeys.NAME, Order.ASC);
        query.order(HugeKeys.FIELDS, Order.DESC);
        Assert.assertEquals("Query for VERTEX page '', offset 100, " +
                            "limit 10, order by {NAME=ASC, FIELDS=DESC}",
                            query.toString());
    }
}
