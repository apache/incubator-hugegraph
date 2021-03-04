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

package com.baidu.hugegraph.backend.store.hbase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableMap;

public class HbaseMetrics implements BackendMetrics {

    private final Connection hbase;

    public HbaseMetrics(HbaseSessions hbase) {
        E.checkArgumentNotNull(hbase, "HBase connection is not opened");
        this.hbase = hbase.hbase();
    }

    @Override
    public Map<String, Object> metrics() {
        Map<String, Object> results = this.clusterInfo();
        if (results.containsKey(EXCEPTION)) {
            return results;
        }

        try (Admin admin = this.hbase.getAdmin()) {
            ClusterMetrics clusterMetrics = admin.getClusterMetrics();
            Map<ServerName, ServerMetrics> metrics =
                            clusterMetrics.getLiveServerMetrics();
            Map<String, Object> regionServers = InsertionOrderUtil.newMap();
            for (Map.Entry<ServerName, ServerMetrics> e : metrics.entrySet()) {
                ServerName server = e.getKey();
                ServerMetrics serverMetrics = e.getValue();
                List<RegionMetrics> regions = admin.getRegionMetrics(server);
                regionServers.put(server.getAddress().toString(),
                                  formatMetrics(serverMetrics, regions));
            }
            results.put(SERVERS, regionServers);
        } catch (Throwable e) {
            results.put(EXCEPTION, e.toString());
        }
        return results;
    }

    public Map<String, Object> compact(List<String> tableNames) {
        Map<String, Object> results = this.clusterInfo();
        if (results.containsKey(EXCEPTION)) {
            return results;
        }

        try (Admin admin = this.hbase.getAdmin()) {
            for (String table : tableNames) {
                admin.compact(TableName.valueOf(table));
            }
            results.put(SERVERS, ImmutableMap.of(SERVER_CLUSTER, "OK"));
        } catch (Throwable e) {
            results.put(EXCEPTION, e.toString());
        }
        return results;
    }

    private Map<String, Object> clusterInfo() {
        Map<String, Object> results = InsertionOrderUtil.newMap();
        try (Admin admin = this.hbase.getAdmin()) {
            // Cluster info
            ClusterMetrics clusterMetrics = admin.getClusterMetrics();
            results.put(CLUSTER_ID, clusterMetrics.getClusterId());
            results.put("average_load", clusterMetrics.getAverageLoad());
            results.put("hbase_version", clusterMetrics.getHBaseVersion());
            results.put("region_count", clusterMetrics.getRegionCount());
            // Region servers info
            Collection<ServerName> servers = admin.getRegionServers();
            results.put(NODES, servers.size());
        } catch (Throwable e) {
            results.put(EXCEPTION, e.toString());
        }
        return results;
    }

    private static Map<String, Object> formatMetrics(
                                       ServerMetrics serverMetrics,
                                       List<RegionMetrics> regions) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        metrics.put("max_heap_size",
                    serverMetrics.getMaxHeapSize().get(Size.Unit.MEGABYTE));
        metrics.put("used_heap_size",
                    serverMetrics.getUsedHeapSize().get(Size.Unit.MEGABYTE));
        metrics.put("heap_size_unit", "MB");
        metrics.put("request_count", serverMetrics.getRequestCount());
        metrics.put("request_count_per_second",
                    serverMetrics.getRequestCountPerSecond());
        metrics.put("regions", formatMetrics(regions));
        return metrics;
    }

    private static Map<String, Object> formatMetrics(
                                       List<RegionMetrics> regions) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        for (RegionMetrics region : regions) {
            metrics.put(region.getNameAsString(), formatMetrics(region));
        }
        return metrics;
    }

    private static Map<String, Object> formatMetrics(RegionMetrics region) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        metrics.put("mem_store_size",
                    region.getMemStoreSize().get(Size.Unit.MEGABYTE));
        metrics.put("file_store_size",
                    region.getStoreFileSize().get(Size.Unit.MEGABYTE));
        metrics.put("store_size_unit", "MB");
        return metrics;
    }
}
