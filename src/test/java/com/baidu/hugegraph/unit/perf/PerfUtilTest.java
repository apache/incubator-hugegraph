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

package com.baidu.hugegraph.unit.perf;

import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.perf.PerfUtil;
import com.baidu.hugegraph.testclass.TestClass.Foo;
import com.baidu.hugegraph.testclass.TestClass.Sub;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PerfUtilTest extends BaseUnitTest {

    private static final PerfUtil perf = PerfUtil.instance();

    @After
    public void teardown() {
        perf.clear();
    }

    @Test
    public void testPerfUtil() throws Exception {
        /*
         * TODO: call profilePackage("com.baidu.hugegraph.testclass") and
         * remove class Foo. now exception "duplicate class definition" throws
         * since JUnit loaded class TestClass before testPerfUtil()
         */
        perf.profilePackage("com.baidu.hugegraph.testclass");
        perf.profileClass("com.baidu.hugegraph.testclass.TestClass$Foo");

        Foo obj = new Foo();
        obj.foo();

        perf.toString();
        perf.toECharts();
        String json = perf.toJson();

        assertContains(json, "foo.times", 1);
        assertContains(json, "foo/bar.times", 1);

        perf.clear();

        obj.foo();
        obj.foo();

        perf.toString();
        perf.toECharts();
        json = perf.toJson();

        assertContains(json, "foo.times", 2);
        assertContains(json, "foo/bar.times", 2);
    }

    @Test
    public void testPerfUtilWithProfileClass() throws Exception {
        perf.profileClass("com.baidu.hugegraph.testclass.TestClass$Base");
        perf.profileClass("com.baidu.hugegraph.testclass.TestClass$Sub");

        Sub obj = new Sub();
        obj.func();
        obj.func1();
        obj.func2();
        obj.func3();
        obj.func3();
        obj.func3();

        String json = perf.toJson();
        assertContains(json, "func.times", 1);
        assertContains(json, "func1.times", 1);
        assertContains(json, "func3.times", 3);
    }

    private static void assertContains(String json, String key, Object value)
            throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<?, ?> map = mapper.readValue(json, Map.class);
        String[] keys = key.split("\\.");
        Object actual = null;
        for (String k : keys) {
            actual = map.get(k);
            if (actual instanceof Map) {
                map = (Map<?, ?>) actual;
            }
        }
        Assert.assertEquals(value, actual);
    }
}
