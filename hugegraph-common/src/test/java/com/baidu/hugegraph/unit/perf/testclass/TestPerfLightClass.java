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

package com.baidu.hugegraph.unit.perf.testclass;

import com.baidu.hugegraph.perf.PerfUtil.Watched;

public class TestPerfLightClass {

    private Foo foo = new Foo();

    @Watched
    public void test(int times) {
        for (int i = 0; i < times; i++) {
            this.testNew();
            this.testNewAndCall();
            this.testCall();
            this.testCallFooThenSum();
        }
    }

    @Watched
    public void testNew() {
        new Foo();
    }

    @Watched
    public void testNewAndCall() {
        new Foo().sum(1, 2);
    }

    @Watched
    public void testCall() {
        this.foo.sum(1, 2);
    }

    @Watched
    public void testCallFooThenSum() {
        this.foo.foo();
    }

    public static class Foo {

        @Watched
        public void foo() {
            this.sum(1, 2);
        }

        @Watched
        public int sum(int a, int b) {
            int sum = a;
            for (int i = 0; i < 100; i++) {
                sum += i;
            }
            return sum + b;
        }
    }
}
