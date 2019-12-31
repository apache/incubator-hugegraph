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

import java.util.function.Consumer;
import java.util.function.Function;

import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;

public class Assert extends org.junit.Assert {

    @FunctionalInterface
    public interface ThrowableRunnable {
        void run() throws Throwable;
    }

    public static void assertThrows(Class<? extends Throwable> throwable,
                                    ThrowableRunnable runnable) {
        assertThrows(throwable, runnable, e -> {
            System.err.println(e);
        });
    }

    public static void assertThrows(Class<? extends Throwable> throwable,
                                    ThrowableRunnable runnable,
                                    Consumer<Throwable> exceptionConsumer) {
        boolean fail = false;
        try {
            runnable.run();
            fail = true;
        } catch (Throwable e) {
            if (!throwable.isInstance(e)) {
                Assert.fail(String.format(
                            "Bad exception type %s(expected %s)",
                            e.getClass().getName(), throwable.getName()));
            }
            exceptionConsumer.accept(e);
        }
        if (fail) {
            Assert.fail(String.format(
                        "No exception was thrown(expected %s)",
                        throwable.getName()));
        }
    }

    public static void assertEquals(byte expected, Object actual) {
        org.junit.Assert.assertEquals(expected, actual);
    }

    public static void assertEquals(short expected, Object actual) {
        org.junit.Assert.assertEquals(expected, actual);
    }

    public static void assertEquals(char expected, Object actual) {
        org.junit.Assert.assertEquals(expected, actual);
    }

    public static void assertEquals(int expected, Object actual) {
        org.junit.Assert.assertEquals(expected, actual);
    }

    public static void assertEquals(long expected, Object actual) {
        org.junit.Assert.assertEquals(expected, actual);
    }

    public static void assertEquals(float expected, Object actual) {
        org.junit.Assert.assertEquals(expected, actual);
    }

    public static void assertEquals(double expected, Object actual) {
        org.junit.Assert.assertEquals(expected, actual);
    }

    public static void assertGt(Number expected, Object actual) {
        org.junit.Assert.assertThat(actual, new NumberMatcher(expected, cmp -> {
            return cmp > 0;
        }, ">"));
    }

    public static void assertGte(Number expected, Object actual) {
        org.junit.Assert.assertThat(actual, new NumberMatcher(expected, cmp -> {
            return cmp >= 0;
        }, ">="));
    }

    public static void assertLt(Number expected, Object actual) {
        org.junit.Assert.assertThat(actual, new NumberMatcher(expected, cmp -> {
            return cmp < 0;
        }, "<"));
    }

    public static void assertLte(Number expected, Object actual) {
        org.junit.Assert.assertThat(actual, new NumberMatcher(expected, cmp -> {
            return cmp <= 0;
        }, "<="));
    }

    public static void assertContains(String sub, String actual) {
        org.junit.Assert.assertThat(actual, CoreMatchers.containsString(sub));
    }

    public static void assertInstanceOf(Class<?> clazz, Object object) {
        org.junit.Assert.assertThat(object, CoreMatchers.instanceOf(clazz));
    }

    private static class NumberMatcher extends BaseMatcher<Object> {

        private final String symbol;
        private final Number expected;
        private final Function<Integer, Boolean> cmp;

        public NumberMatcher(Number expected, Function<Integer, Boolean> cmp,
                             String symbol) {
            this.expected = expected;
            this.cmp = cmp;
            this.symbol = symbol;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean matches(Object actual) {
            Assert.assertInstanceOf(this.expected.getClass(), actual);
            Assert.assertInstanceOf(Comparable.class, actual);
            int cmp = ((Comparable<Number>) actual).compareTo(this.expected);
            return this.cmp.apply(cmp);
        }

        @Override
        public void describeTo(Description desc) {
            desc.appendText("a number ").appendText(this.symbol)
                .appendText(" ").appendText(this.expected.toString());
        }
    }
}
