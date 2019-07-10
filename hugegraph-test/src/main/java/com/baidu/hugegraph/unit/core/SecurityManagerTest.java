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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.job.GremlinAPI;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.security.HugeSecurityManager;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.FakeObjects;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableMap;

public class SecurityManagerTest {

    private static HugeGraph graph;

    @BeforeClass
    public static void init() {
        graph = loadGraph(false);
        runGremlinJob("1 + 1");
        System.setSecurityManager(new HugeSecurityManager());
    }

    @AfterClass
    public static void clear() {
        System.setSecurityManager(null);
        graph.close();
        HugeGraph.shutdown(30L);
    }

    @Test
    public void testNormal() {
        String result = runGremlinJob("g.V()");
        Assert.assertEquals("[]", result);

        result = runGremlinJob("1 + 2");
        Assert.assertEquals("3", result);
    }

    @Test
    public void testThread() {
        String result = runGremlinJob("new Thread()");
        assertError(result, "Not allowed to access thread group via Gremlin");

        result = runGremlinJob("Thread.currentThread().stop()");
        assertError(result, "Not allowed to access thread via Gremlin");
    }

    @Test
    public void testExit() {
        String result = runGremlinJob("System.exit(-1)");
        assertError(result, "Not allowed to call System.exit() via Gremlin");
    }

    @Test
    public void testFile() {
        String result = runGremlinJob("fr = new FileReader(new File(\"\"));" +
                                      "fr.read();fr.close()");
        assertError(result, "Not allowed to read file via Gremlin");

        result = runGremlinJob("fis = new FileInputStream(FileDescriptor.in);" +
                               "fis.read();fis.close()");
        assertError(result, "Not allowed to read fd via Gremlin");

        result = runGremlinJob("fw = new FileWriter(new File(\"\"));" +
                               "fw.write(1);fw.close()");
        assertError(result, "Not allowed to write file via Gremlin");

        result = runGremlinJob("fos = new FileOutputStream(" +
                               "FileDescriptor.out);" +
                               "fos.write(\"abcd\".getBytes());" +
                               "fos.close()");
        assertError(result, "Not allowed to write fd via Gremlin");

        result = runGremlinJob("new File(\"\").delete()");
        assertError(result, "Not allowed to delete file via Gremlin");
    }

//    @Test
    public void testSocket() {
        String result = runGremlinJob("new ServerSocket(8200)");
        assertError(result, "Not allowed to listen socket via Gremlin");

        result = runGremlinJob("new Socket().connect(" +
                               "new InetSocketAddress(\"localhost\", 8200))");
        assertError(result, "Not allowed to connect socket via Gremlin");
    }

    @Test
    public void testExec() {
        String result = runGremlinJob("process=Runtime.getRuntime().exec(" +
                                      "'cat /etc/passwd'); process.waitFor()");
        assertError(result, "Not allowed to execute command via Gremlin");
    }

    @Test
    public void testLink() {
        String result = runGremlinJob("Runtime.getRuntime().loadLibrary" +
                                      "(\"test.jar\")");
        assertError(result, "Not allowed to link library via Gremlin");
    }

    @Test
    public void testProperties() {
        String result = runGremlinJob("System.getProperties()");
        assertError(result,
                    "Not allowed to access system properties via Gremlin");

        result = runGremlinJob("System.getProperty(\"java.version\")");
        assertError(result,
                    "Not allowed to access system property(java.version) " +
                    "via Gremlin");
    }

    private static void assertError(String result, String message) {
        Assert.assertTrue(result, result.endsWith(message));
    }

    public static String runGremlinJob(String gremlin) {
        JobBuilder<Object> builder = JobBuilder.of(graph);
        Map<String, Object> input = new HashMap<>();
        input.put("gremlin", gremlin);
        input.put("bindings", ImmutableMap.of());
        input.put("language", "gremlin-groovy");
        input.put("aliases", ImmutableMap.of());
        builder.name("test-gremlin-job")
               .input(JsonUtil.toJson(input))
               .job(new GremlinAPI.GremlinJob());
        HugeTask<?> task = builder.schedule();
        try {
            graph.taskScheduler().waitUntilTaskCompleted(task.id(), 5);
        } catch (TimeoutException e) {
            throw new HugeException("Wait task %s timeout", e, task);
        }
        return task.result();
    }

    public static HugeGraph loadGraph(boolean needClear) {
        HugeConfig config = FakeObjects.newConfig();
        HugeGraph graph = new HugeGraph(config);

        if (needClear) {
            graph.clearBackend();
        }
        graph.initBackend();

        return graph;
    }
}
