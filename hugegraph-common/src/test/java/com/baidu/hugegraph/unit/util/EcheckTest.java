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

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class EcheckTest extends BaseUnitTest {

    @Test
    public void testCheckNotNull() {
        E.checkNotNull(0, "test");
        E.checkNotNull(new Object(), "test");
        E.checkNotNull("1", "test");
        E.checkNotNull(ImmutableList.of(), "test");

        Assert.assertThrows(NullPointerException.class, () -> {
            E.checkNotNull(null, "test");
        }, e -> {
            Assert.assertContains("The 'test' can't be null", e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            E.checkNotNull(null, "test2");
        }, e -> {
            Assert.assertContains("The 'test2' can't be null", e.getMessage());
        });
    }

    @Test
    public void testCheckNotNullWithOwner() {
        E.checkNotNull(0, "test", "obj");
        E.checkNotNull(new Object(), "test", "obj");
        E.checkNotNull("1", "test", "obj");
        E.checkNotNull(ImmutableList.of(), "test", "obj");

        Assert.assertThrows(NullPointerException.class, () -> {
            E.checkNotNull(null, "test", "obj");
        }, e -> {
            Assert.assertContains("The 'test' of 'obj' can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            E.checkNotNull(null, "test2", "obj2");
        }, e -> {
            Assert.assertContains("The 'test2' of 'obj2' can't be null",
                                  e.getMessage());
        });
    }

    @Test
    public void testCheckNotEmpty() {
        E.checkNotEmpty(ImmutableList.of(0), "test");
        E.checkNotEmpty(ImmutableList.of(""), "test");
        E.checkNotEmpty(ImmutableList.of(1, 2), "test");
        E.checkNotEmpty(ImmutableSet.of(0), "test");
        E.checkNotEmpty(ImmutableSet.of(""), "test");
        E.checkNotEmpty(ImmutableSet.of("1", "2"), "test");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            E.checkNotEmpty(ImmutableList.of(), "test");
        }, e -> {
            Assert.assertContains("The 'test' can't be empty", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            E.checkNotEmpty(ImmutableSet.of(), "test2");
        }, e -> {
            Assert.assertContains("The 'test2' can't be empty", e.getMessage());
        });
    }

    @Test
    public void testCheckNotEmptyWithOwner() {
        E.checkNotEmpty(ImmutableList.of(0), "test", "obj");
        E.checkNotEmpty(ImmutableList.of(""), "test", "obj");
        E.checkNotEmpty(ImmutableList.of(1, 2), "test", "obj");
        E.checkNotEmpty(ImmutableSet.of(0), "test", "obj");
        E.checkNotEmpty(ImmutableSet.of(""), "test", "obj");
        E.checkNotEmpty(ImmutableSet.of("1", "2"), "test", "obj");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            E.checkNotEmpty(ImmutableList.of(), "test", "obj");
        }, e -> {
            Assert.assertContains("The 'test' of 'obj' can't be empty",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            E.checkNotEmpty(ImmutableSet.of(), "test2", "obj2");
        }, e -> {
            Assert.assertContains("The 'test2' of 'obj2' can't be empty",
                                  e.getMessage());
        });
    }

    @Test
    public void testCheckArgument() {
        E.checkArgument(true, "test");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            E.checkArgument(false, "Invalid parameter %s", 123);
        }, e -> {
            Assert.assertContains("Invalid parameter 123", e.getMessage());
        });
    }

    @Test
    public void testCheckArgumentNotNull() {
        E.checkArgumentNotNull("", "test");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            E.checkArgumentNotNull(null, "Invalid parameter %s", "null");
        }, e -> {
            Assert.assertContains("Invalid parameter null", e.getMessage());
        });
    }

    @Test
    public void testCheckState() {
        E.checkState(true, "test");

        Assert.assertThrows(IllegalStateException.class, () -> {
            E.checkState(false, "Invalid state '%s'", "FAIL");
        }, e -> {
            Assert.assertContains("Invalid state 'FAIL'", e.getMessage());
        });
    }
}
