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
                assertMapContains(host, "mem_used_readable");
                assertMapContains(host, "mem_unit");

                assertMapContains(host, "disk_usage");
                assertMapContains(host, "disk_usage_readable");
                assertMapContains(host, "disk_unit");

                assertMapContains(host, "estimate_num_keys");
                break;
            case "cassandra":
                assertMapContains(graph, "cluster_id");
                assertMapContains(graph, "servers");

                servers = (Map<?, ?>) graph.get("servers");
                Assert.assertGte(1, servers.size());
                for (Map.Entry<?, ?> e : servers.entrySet()) {
                    String key = (String) e.getKey();
                    value = e.getValue();
                    Assert.assertTrue(String.format(
                                      "Expect map value for key %s but got %s",
                                      key, value),
                                      value instanceof Map);
                    host = (Map<?, ?>) value;
                    assertMapContains(host, "mem_max");
                    assertMapContains(host, "mem_committed");
                    assertMapContains(host, "mem_used");
                    assertMapContains(host, "mem_used_readable");
                    assertMapContains(host, "mem_unit");

                    assertMapContains(host, "disk_usage");
                    assertMapContains(host, "disk_usage_readable");
                    assertMapContains(host, "disk_usage_details");
                    assertMapContains(host, "disk_unit");

                    assertMapContains(host, "uptime");
                    assertMapContains(host, "uptime_readable");
                    assertMapContains(host, "time_unit");

                    assertMapContains(host, "estimated_partition_count");
                    assertMapContains(host, "dropped_mutations");
                    assertMapContains(host, "pending_flushes");
                    assertMapContains(host, "key_cache_hit_rate");
                    assertMapContains(host, "bloom_filter_false_ratio");

                    assertMapContains(host, "write_latency_hugegraph");
                    assertMapContains(host, "read_latency_hugegraph");
                    assertMapContains(host, "write_latency_*");
                    assertMapContains(host, "read_latency_*");

                    assertMapContains(host, "key_cache_size");
                    assertMapContains(host, "key_cache_entries");
                    assertMapContains(host, "row_cache_size");
                    assertMapContains(host, "row_cache_entries");
                    assertMapContains(host, "counter_cache_size");
                    assertMapContains(host, "counter_cache_entries");

                    assertMapContains(host, "compaction_completed_tasks");
                    assertMapContains(host, "compaction_pending_tasks");
                    assertMapContains(host, "compaction_bytes_compacted");

                    assertMapContains(host, "live_nodes");
                    assertMapContains(host, "joining_nodes");
                    assertMapContains(host, "moving_nodes");
                    assertMapContains(host, "leaving_nodes");
                    assertMapContains(host, "unreachable_nodes");

                    assertMapContains(host, "keyspaces");
                    assertMapContains(host, "num_tables");
                    assertMapContains(host, "exception_count");
                }
                break;
            case "scylladb":
                assertMapContains(graph, "cluster_id");
                assertMapContains(graph, "servers");

                servers = (Map<?, ?>) graph.get("servers");
                Assert.assertGte(1, servers.size());
                for (Map.Entry<?, ?> e : servers.entrySet()) {
                    String key = (String) e.getKey();
                    value = e.getValue();
                    Assert.assertTrue(String.format(
                                      "Expect map value for key %s but got %s",
                                      key, value),
                                      value instanceof Map);
                    host = (Map<?, ?>) value;
                    assertMapContains(host, "mem_max");
                    assertMapContains(host, "mem_committed");
                    assertMapContains(host, "mem_used");
                    assertMapContains(host, "mem_used_readable");
                    assertMapContains(host, "mem_unit");

                    assertMapContains(host, "disk_usage");
                    assertMapContains(host, "disk_usage_readable");
                    assertMapContains(host, "disk_usage_details");
                    assertMapContains(host, "disk_unit");

                    assertMapContains(host, "uptime");
                    assertMapContains(host, "uptime_readable");
                    assertMapContains(host, "time_unit");

                    assertMapContains(host, "estimated_partition_count");
                    assertMapContains(host, "dropped_mutations");
                    assertMapContains(host, "pending_flushes");
                    //assertMapContains(host, "key_cache_hit_rate");
                    assertMapContains(host, "bloom_filter_false_ratio");

                    //assertMapContains(host, "write_latency_hugegraph");
                    //assertMapContains(host, "read_latency_hugegraph");
                    //assertMapContains(host, "write_latency_*");
                    //assertMapContains(host, "read_latency_*");

                    assertMapContains(host, "key_cache_size");
                    assertMapContains(host, "key_cache_entries");
                    assertMapContains(host, "row_cache_size");
                    assertMapContains(host, "row_cache_entries");
                    assertMapContains(host, "counter_cache_size");
                    assertMapContains(host, "counter_cache_entries");

                    assertMapContains(host, "compaction_completed_tasks");
                    assertMapContains(host, "compaction_pending_tasks");
                    //assertMapContains(host, "compaction_bytes_compacted");

                    assertMapContains(host, "live_nodes");
                    assertMapContains(host, "joining_nodes");
                    assertMapContains(host, "moving_nodes");
                    assertMapContains(host, "leaving_nodes");
                    assertMapContains(host, "unreachable_nodes");

                    assertMapContains(host, "keyspaces");
                    assertMapContains(host, "num_tables");
                    assertMapContains(host, "exception_count");
                }
                break;
            case "hbase":
                assertMapContains(graph, "cluster_id");
                assertMapContains(graph, "master_name");
                assertMapContains(graph, "average_load");
                assertMapContains(graph, "hbase_version");
                assertMapContains(graph, "region_count");
                assertMapContains(graph, "leaving_servers");
                assertMapContains(graph, "nodes");
                assertMapContains(graph, "region_servers");

                assertMapContains(graph, "servers");
                servers = (Map<?, ?>) graph.get("servers");
                Assert.assertGte(1, servers.size());
                for (Map.Entry<?, ?> e : servers.entrySet()) {
                    String key = (String) e.getKey();
                    value = e.getValue();
                    Assert.assertTrue(String.format(
                                      "Expect map value for key %s but got %s",
                                      key, value),
                                      value instanceof Map);
                    Map<?, ?> regionServer = (Map<?, ?>) value;
                    assertMapContains(regionServer, "mem_max");
                    assertMapContains(regionServer, "mem_used");
                    assertMapContains(regionServer, "mem_used_readable");
                    assertMapContains(regionServer, "mem_unit");

                    assertMapContains(regionServer, "disk_usage");
                    assertMapContains(regionServer, "disk_usage_readable");
                    assertMapContains(regionServer, "disk_unit");

                    assertMapContains(regionServer, "request_count");
                    assertMapContains(regionServer, "request_count_per_second");
                    assertMapContains(regionServer, "coprocessor_names");

                    Map<?, ?> regions = assertMapContains(regionServer,
                                                          "regions");
                    Assert.assertGte(1, regions.size());
                    for (Map.Entry<?, ?> e2 : regions.entrySet()) {
                        Map<?, ?> region = (Map<?, ?>) e2.getValue();
                        assertMapContains(region, "disk_usage");
                        assertMapContains(region, "disk_usage_readable");
                        assertMapContains(region, "disk_unit");

                        assertMapContains(region, "index_store_size");
                        assertMapContains(region, "root_level_index_store_size");
                        assertMapContains(region, "mem_store_size");
                        assertMapContains(region, "bloom_filter_size");
                        assertMapContains(region, "size_unit");

                        assertMapContains(region, "store_count");
                        assertMapContains(region, "store_file_count");

                        assertMapContains(region, "request_count");
                        assertMapContains(region, "write_request_count");
                        assertMapContains(region, "read_request_count");
                        assertMapContains(region, "filtered_read_request_count");

                        assertMapContains(region, "completed_sequence_id");
                        assertMapContains(region, "data_locality");
                        assertMapContains(region, "compacted_cell_count");
                        assertMapContains(region, "compacting_cell_count");
                        assertMapContains(region, "last_compaction_time");
                    }
                }
                break;
            default:
                Assert.assertTrue("Unexpected backend " + backend, false);
                break;
        }
    }
}
