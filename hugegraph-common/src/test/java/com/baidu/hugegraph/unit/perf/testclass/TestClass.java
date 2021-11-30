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

import com.baidu.hugegraph.perf.PerfUtil;
import com.baidu.hugegraph.perf.PerfUtil.Watched;

public class TestClass {

    @Watched
    public void test() {
        new Foo().bar();
    }

    public static class Foo {

        @Watched(prefix="foo")
        public void foo() {
            this.bar();
        }

        @Watched(prefix="foo")
        public void bar() {}
    }

    public static class Bar {

        @Watched("bar_foo")
        public void foo() {
            this.bar();
        }

        @Watched("bar_bar")
        public void bar() {}
    }

    public static class ManuallyProfile {

        public void foo() {
            PerfUtil.instance().start("manu-foo");
            this.bar();
            this.bar2();
            PerfUtil.instance().end("manu-foo");
        }

        public void bar() {
            PerfUtil.instance().start("manu-bar");
            try {
                Thread.sleep(0);
            } catch (InterruptedException ignored) {
                // pass
            }
            PerfUtil.instance().end("manu-bar");
        }

        public void foo2() {
            PerfUtil.instance().start2("manu-foo2");
            this.bar();
            this.bar2();
            PerfUtil.instance().end("manu-foo2");
        }

        public void bar2() {
            PerfUtil.instance().start2("manu-bar2");
            try {
                Thread.sleep(0);
            } catch (InterruptedException ignored) {
                // pass
            }
            PerfUtil.instance().end("manu-bar2");
        }
    }

    public static class Base {

        @Watched
        public void func() {}
    }

    public static class Sub extends Base {

        @Watched
        public void func1() {}

        public void func2() {}

        @Watched
        public void func3() {}
    }
}
