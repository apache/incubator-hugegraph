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
            exceptionConsumer.accept(e);
            if (!throwable.isInstance(e)) {
                Assert.fail(String.format(
                            "Bad exception type %s(expect %s)",
                            e.getClass(), throwable));
            }
        }
        if (fail) {
            Assert.fail(String.format(
                        "No exception was thrown(expect %s)",
                        throwable));
        }
    }
}
