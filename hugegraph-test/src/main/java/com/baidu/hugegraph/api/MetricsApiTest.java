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

package com.baidu.hugegraph.api;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

import jersey.repackaged.com.google.common.collect.ImmutableMap;


public class MetricsApiTest extends BaseApiTest {

    private static String path = "/metrics";

    @Test
    public void testMetricsAll() {
        Response r = client().get(path, ImmutableMap.of("type", "json"));
        String result = assertResponseStatus(200, r);
        assertJsonContains(result, "gauges");
        assertJsonContains(result, "counters");
        assertJsonContains(result, "histograms");
        assertJsonContains(result, "meters");
        assertJsonContains(result, "timers");
    }

    @Test
    public void testMetricsSystem() {
        Response r = client().get(path, "system");
        String result = assertResponseStatus(200, r);
        assertJsonContains(result, "basic");
        assertJsonContains(result, "heap");
        assertJsonContains(result, "nonheap");
        assertJsonContains(result, "thread");
        assertJsonContains(result, "class_loading");
        assertJsonContains(result, "garbage_collector");
    }

    @Test
    public void testMetricsBackend() {
        Response r = client().get(path, "backend");
        String result = assertResponseStatus(200, r);
        Object value = assertJsonContains(result, "hugegraph");

        Assert.assertTrue(value instanceof Map);
        Map<?, ?> graph = (Map<?, ?>) value;
        assertMapContains(graph, "backend");
        assertMapContains(graph, "nodes");
        String backend = (String) graph.get("backend");
        int nodes = (Integer) graph.get("nodes");
        switch (backend) {
            case "memory":
            case "rocksdb":
                Assert.assertEquals(1, nodes);

                String clusterId = assertMapContains(graph, "cluster_id");
                Assert.assertEquals("local", clusterId);

                Map<?, ?> servers = assertMapContains(graph, "servers");
                Assert.assertEquals(1, servers.size());
                Map.Entry<?, ?> server = servers.entrySet().iterator().next();
                Assert.assertEquals("local", server.getKey());

                Map<?, ?> host = (Map<?, ?>) server.getValue();
                assertMapContains(host, "mem_used");
                assertMapContains(host, "mem_used_readable");
                assertMapContains(host, "mem_unit");

                assertMapContains(host, "disk_usage");
                assertMapContains(host, "disk_usage_readable");
                assertMapContains(host, "disk_unit");

                assertMapContains(host, "estimate_num_keys");
                break;
            default:
                Assert.assertTrue("Unexpected backend " + backend, false);
                break;
        }
    }

//    There is no default graph in v3.0.0, disable this test case
//    @Test
//    public void testPrometheusAll() {
//
//        Response r = client().get(path, ImmutableMap.of());
//        String result = assertResponseStatus(200, r);
//        Assert.assertContains(
//                "com_baidu_hugegraph_vgraph_VirtualGraph_DEFAULT_hugegraph_hits", result);
//    }
}
