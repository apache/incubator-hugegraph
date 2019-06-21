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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.ConditionQueryFlatten;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ConditionQueryFlattenTest extends BaseUnitTest {

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testFlattenWithAnd() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1.and(c2));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(1, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1, c2));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithMultiAnd() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");
        Condition c3 = Condition.eq(IdGenerator.of("c3"), "3");
        Condition c4 = Condition.eq(IdGenerator.of("c4"), "4");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1);
        query.query(c2);
        query.query(c3);
        query.query(c4);
        Assert.assertEquals(4, query.conditions().size());
        List<ConditionQuery> queries =
                ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(1, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1, c2, c3, c4));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithAndTree() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");
        Condition c3 = Condition.eq(IdGenerator.of("c3"), "3");
        Condition c4 = Condition.eq(IdGenerator.of("c4"), "4");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1.and(c2).and(c3.and(c4)));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(1, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1, c2, c3, c4));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithOr() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1.or(c2));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(2, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1), ImmutableSet.of(c2));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithMultiOr() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");
        Condition c3 = Condition.eq(IdGenerator.of("c3"), "3");
        Condition c4 = Condition.eq(IdGenerator.of("c4"), "4");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1.or(c2));
        query.query(c3.or(c4));
        Assert.assertEquals(2, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(4, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1, c3),
                                 ImmutableSet.of(c1, c4),
                                 ImmutableSet.of(c2, c3),
                                 ImmutableSet.of(c2, c4));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithOrTree() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");
        Condition c3 = Condition.eq(IdGenerator.of("c3"), "3");
        Condition c4 = Condition.eq(IdGenerator.of("c4"), "4");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1.or(c2).or(c3.or(c4)));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(4, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1), ImmutableSet.of(c2),
                                 ImmutableSet.of(c3), ImmutableSet.of(c4));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithAndOrTree() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");
        Condition c3 = Condition.eq(IdGenerator.of("c3"), "3");
        Condition c4 = Condition.eq(IdGenerator.of("c4"), "4");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1.or(c2).and(c3.or(c4)));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(4, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1, c3),
                                 ImmutableSet.of(c1, c4),
                                 ImmutableSet.of(c2, c3),
                                 ImmutableSet.of(c2, c4));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithOrAndTree() {
        Condition c1 = Condition.eq(IdGenerator.of("c1"), "1");
        Condition c2 = Condition.eq(IdGenerator.of("c2"), "2");
        Condition c3 = Condition.eq(IdGenerator.of("c3"), "3");
        Condition c4 = Condition.eq(IdGenerator.of("c4"), "4");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(c1.and(c2).or(c3.and(c4)));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(2, queries.size());
        Set<Set<Condition>> expect;
        expect = ImmutableSet.of(ImmutableSet.of(c1, c2),
                                 ImmutableSet.of(c3, c4));
        Set<Set<Condition>> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            actual.add(q.conditions());
        }
        Assert.assertEquals(expect, actual);
    }


    @Test
    public void testFlattenWithIn() {
        Id key = IdGenerator.of("c1");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.in(key, ImmutableList.of("1", "2", "3")));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(3, queries.size());

        Set<Condition> expect = ImmutableSet.of(Condition.eq(key, "1"),
                                                Condition.eq(key, "2"),
                                                Condition.eq(key, "3"));
        Set<Condition> actual = new HashSet<>();
        for (ConditionQuery q : queries) {
            Assert.assertEquals(1, q.conditions().size());
            actual.add(q.conditions().iterator().next());
        }

        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testFlattenWithNotIn() {
        Id key = IdGenerator.of("c1");

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.nin(key, ImmutableList.of("1", "2", "3")));
        Assert.assertEquals(1, query.conditions().size());
        List<ConditionQuery> queries = ConditionQueryFlatten.flatten(query);
        Assert.assertEquals(1, queries.size());

        Set<Condition> expect = ImmutableSet.of(Condition.neq(key, "1"),
                                                Condition.neq(key, "2"),
                                                Condition.neq(key, "3"));
        Set<Condition> actual = queries.iterator().next().conditions();
        Assert.assertEquals(expect, actual);
    }
}

