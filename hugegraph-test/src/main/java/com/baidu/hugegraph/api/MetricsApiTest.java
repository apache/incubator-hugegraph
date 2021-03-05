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

public class MetricsApiTest extends BaseApiTest {

    private static String path = "/metrics";

    @Test
    public void testMetricsAll() {
        Response r = client().get(path);
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
            case "mysql":
            case "postgresql":
                Assert.assertEquals(1, nodes);
                break;
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
                assertMapContains(host, "mem_unit");
                assertMapContains(host, "disk_usage");
                assertMapContains(host, "disk_unit");
                break;
            case "cassandra":
            case "scylladb":
                assertMapContains(graph, "cluster_id");
                assertMapContains(graph, "servers");

                servers = (Map<?, ?>) graph.get("servers");
                for (Map.Entry<?, ?> e : servers.entrySet()) {
                    String key = (String) e.getKey();
                    value = e.getValue();
                    Assert.assertTrue(String.format(
                                      "Expect map value for key %s but got %s",
                                      key, value),
                                      value instanceof Map);
                    host = (Map<?, ?>) value;
                    assertMapContains(host, "mem_used");
                    assertMapContains(host, "mem_commited");
                    assertMapContains(host, "mem_max");
                    assertMapContains(host, "mem_unit");
                    assertMapContains(host, "disk_usage");
                    assertMapContains(host, "disk_unit");
                }
                break;
            case "hbase":
                assertMapContains(graph, "cluster_id");
                assertMapContains(graph, "average_load");
                assertMapContains(graph, "hbase_version");
                assertMapContains(graph, "region_count");
                assertMapContains(graph, "region_servers");
                assertMapContains(graph, "servers");
                servers = (Map<?, ?>) graph.get("servers");
                for (Map.Entry<?, ?> e : servers.entrySet()) {
                    String key = (String) e.getKey();
                    value = e.getValue();
                    Assert.assertTrue(String.format(
                                      "Expect map value for key %s but got %s",
                                      key, value),
                                      value instanceof Map);
                    Map<?, ?> regionServer = (Map<?, ?>) value;
                    assertMapContains(regionServer, "mem_used");
                    assertMapContains(regionServer, "mem_max");
                    assertMapContains(regionServer, "mem_unit");
                    assertMapContains(regionServer, "request_count");
                    assertMapContains(regionServer, "request_count_per_second");

                    Map<?, ?> regions = assertMapContains(regionServer,
                                                          "regions");
                    for (Map.Entry<?, ?> e2 : regions.entrySet()) {
                        Map<?, ?> region = (Map<?, ?>) e2.getValue();
                        assertMapContains(region, "mem_store_size");
                        assertMapContains(region, "disk_usage");
                        assertMapContains(region, "disk_unit");
                    }
                }
                break;
            default:
                Assert.assertTrue("Unexpected backend " + backend, false);
                break;
        }
    }
}
