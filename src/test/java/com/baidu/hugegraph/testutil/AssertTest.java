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

package com.baidu.hugegraph.testutil;

import org.junit.Test;

import com.baidu.hugegraph.unit.BaseUnitTest;

public class AssertTest extends BaseUnitTest {

    @Test
    public void testAssertEquals() {
        Assert.assertEquals((byte) 1, Byte.valueOf("1"));
        Assert.assertEquals((short) 1, Short.valueOf("1"));
        Assert.assertEquals('1', Character.valueOf('1'));
        Assert.assertEquals(1, Integer.valueOf("1"));
        Assert.assertEquals(1L, Long.valueOf("1"));
        Assert.assertEquals(1f, Float.valueOf("1"));
        Assert.assertEquals(1d, Double.valueOf("1"));
    }

    @Test
    public void testAssertThrows() {
        Assert.assertThrows(NullPointerException.class, () -> {
            throw new NullPointerException();
        });
        Assert.assertThrows(RuntimeException.class, () -> {
            throw new RuntimeException();
        });

        Assert.assertThrows(RuntimeException.class, () -> {
            throw new RuntimeException("fake-error");
        }, e -> {
            Assert.assertEquals("fake-error", e.getMessage());
        });
    }

    @Test
    public void testAssertThrowsWithError() {
        try {
            Assert.assertThrows(NullPointerException.class, () -> {
                // pass
            });
            Assert.fail("Expect error");
        } catch (AssertionError e) {
            Assert.assertEquals("No exception was thrown" +
                                "(expect class java.lang.NullPointerException)",
                                e.getMessage());
        }

        try {
            Assert.assertThrows(NullPointerException.class, () -> {
                throw new RuntimeException();
            });
            Assert.fail("Expect error");
        } catch (AssertionError e) {
            Assert.assertEquals("Bad exception type class " +
                                "java.lang.RuntimeException" +
                                "(expect class java.lang.NullPointerException)",
                                e.getMessage());
        }
    }
}
