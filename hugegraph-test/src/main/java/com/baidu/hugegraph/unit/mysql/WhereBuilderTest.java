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

package com.baidu.hugegraph.unit.mysql;

import org.junit.Test;

import com.baidu.hugegraph.backend.query.Condition.RelationType;
import com.baidu.hugegraph.backend.store.mysql.WhereBuilder;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableList;

public class WhereBuilderTest extends BaseUnitTest {

    @Test
    public void testRelation() {
        WhereBuilder where = new WhereBuilder();
        where.relation("key1", RelationType.EQ, "value1");
        Assert.assertEquals(" WHERE key1='value1'", where.build().toString());
        Assert.assertEquals(" WHERE key1='value1'", where.toString());

        where = new WhereBuilder(false);
        where.relation("key1", RelationType.EQ, "value1");
        Assert.assertEquals(" key1='value1'", where.build().toString());
        Assert.assertEquals(" key1='value1'", where.toString());

        where.and().relation("key2", RelationType.EQ, "value2");
        Assert.assertEquals(" key1='value1' AND key2='value2'",
                            where.toString());

        where = new WhereBuilder(false);
        where.relation("key1", RelationType.NEQ, "value1");
        Assert.assertEquals(" key1!='value1'", where.toString());

        where = new WhereBuilder(false);
        where.relation("key1", RelationType.GT, "value1");
        Assert.assertEquals(" key1>'value1'", where.toString());

        where = new WhereBuilder(false);
        where.relation("key1", RelationType.GTE, "value1");
        Assert.assertEquals(" key1>='value1'", where.toString());

        where = new WhereBuilder(false);
        where.relation("key1", RelationType.LT, "value1");
        Assert.assertEquals(" key1<'value1'", where.toString());

        where = new WhereBuilder(false);
        where.relation("key1", RelationType.LTE, "value1");
        Assert.assertEquals(" key1<='value1'", where.toString());

        where = new WhereBuilder(false);
        where.relation("key1", RelationType.IN, ImmutableList.of("v1", "v2"));
        Assert.assertEquals(" key1 IN ('v1', 'v2')", where.toString());

        Assert.assertThrows(NotSupportException.class, () -> {
            new WhereBuilder().relation("k", RelationType.CONTAINS_KEY, "v");
        });
        Assert.assertThrows(NotSupportException.class, () -> {
            new WhereBuilder().relation("k", RelationType.CONTAINS_VALUE, "v");
        });
        Assert.assertThrows(NotSupportException.class, () -> {
            new WhereBuilder().relation("k", RelationType.NOT_IN, "v");
        });
        Assert.assertThrows(NotSupportException.class, () -> {
            new WhereBuilder().relation("k", RelationType.TEXT_CONTAINS, "v");
        });
        Assert.assertThrows(NotSupportException.class, () -> {
            new WhereBuilder().relation("k", RelationType.SCAN, "v");
        });
    }

    @Test
    public void testAnd() {
        WhereBuilder where = new WhereBuilder(false);
        where.and(ImmutableList.of("k1", "k2"), ImmutableList.of("v1", "v2"));
        Assert.assertEquals(" k1='v1' AND k2='v2'", where.toString());

        where = new WhereBuilder(false);
        where.and(ImmutableList.of("k1", "k2"), "!=",
                  ImmutableList.of("v1", "v2"));
        Assert.assertEquals(" k1!='v1' AND k2!='v2'", where.toString());

        where = new WhereBuilder(false);
        where.and(ImmutableList.of("k1", "k2", "k3"),
                  ImmutableList.of("=", "!=", ">"),
                  ImmutableList.of("v1", "v2", 3));
        Assert.assertEquals(" k1='v1' AND k2!='v2' AND k3>3", where.toString());

        where = new WhereBuilder(false);
        where.and(ImmutableList.of("k1", "k2"), "=");
        Assert.assertEquals(" k1=? AND k2=?", where.toString());
    }

    @Test
    public void testIn() {
        WhereBuilder where = new WhereBuilder(false);
        where.in("key", ImmutableList.of("v1", "v2", "v3"));
        Assert.assertEquals(" key IN ('v1', 'v2', 'v3')", where.toString());
    }

    @Test
    public void testGt() {
        WhereBuilder where = new WhereBuilder(false);
        where.gte(ImmutableList.of("k1", "k2"), ImmutableList.of("v1", "v2"));
        Assert.assertEquals(" (k1, k2) >= ('v1', 'v2')", where.toString());
    }
}
