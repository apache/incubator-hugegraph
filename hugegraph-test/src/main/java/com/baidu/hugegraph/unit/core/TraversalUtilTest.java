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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;

public class TraversalUtilTest {

    @Test
    public void testParsePredicate() {
        Assert.assertEquals(P.eq(0),
                            TraversalUtil.parsePredicate("P.eq(0)"));
        Assert.assertEquals(P.eq(-1),
                            TraversalUtil.parsePredicate("P.eq( -1 )"));
        Assert.assertEquals(P.neq(0),
                            TraversalUtil.parsePredicate("P.neq(0)"));

        Assert.assertEquals(P.lt(-1),
                            TraversalUtil.parsePredicate("P.lt(-1)"));
        Assert.assertEquals(P.lte(-123.45),
                            TraversalUtil.parsePredicate("P.lte(-123.45)"));

        Assert.assertEquals(P.gt(18),
                            TraversalUtil.parsePredicate("P.gt(18)"));
        Assert.assertEquals(P.gte(3.14),
                            TraversalUtil.parsePredicate("P.gte(3.14)"));

        Assert.assertEquals(P.between(1, 100),
                            TraversalUtil.parsePredicate("P.between(1, 100)"));
        Assert.assertEquals(P.inside(1, 100),
                            TraversalUtil.parsePredicate("P.inside(1, 100)"));
        Assert.assertEquals(P.outside(1, 100),
                            TraversalUtil.parsePredicate("P.outside(1, 100)"));

        Assert.assertEquals(P.within(1, 3, 5),
                            TraversalUtil.parsePredicate("P.within(1, 3, 5)"));
        Assert.assertEquals(P.within("abc", "hello", (Object) 123, 3.14),
                            TraversalUtil.parsePredicate(
                            "P.within(\"abc\", \"hello\", 123, 3.14)"));
    }

    @Test
    public void testParsePredicateWithInvalidString() {
        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("what(123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: what(123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.what(123)");
        }, e -> {
            Assert.assertEquals("Not support predicate 'what'",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P .eq(123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: P .eq(123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P. eq(123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: P. eq(123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq (123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: P.eq (123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.neq(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                               e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.lt(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.lte(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.gt(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.gte(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.between(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.between(\"18m\", 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '\"18m\", 20', " +
                                "expect a list of number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.inside(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.inside(\"18m\", 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '\"18m\", 20', " +
                                "expect a list of number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.outside(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.outside(\"18m\", 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '\"18m\", 20', " +
                                "expect a list of number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.within(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });
    }
}
