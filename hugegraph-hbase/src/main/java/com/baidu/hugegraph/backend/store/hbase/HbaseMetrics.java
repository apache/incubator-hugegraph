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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.client.Admin;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.UnitUtil;
import com.google.common.collect.ImmutableMap;

public class HbaseMetrics implements BackendMetrics {

    private final HbaseSessions hbase;

    public HbaseMetrics(HbaseSessions hbase) {
        E.checkArgumentNotNull(hbase, "HBase connection is not opened");
        this.hbase = hbase;
    }

    @Override
    public Map<String, Object> metrics() {
        Map<String, Object> results = this.clusterInfo();
        if (results.containsKey(EXCEPTION)) {
            return results;
        }

        try (Admin admin = this.hbase.hbase().getAdmin()) {
            ClusterMetrics clusterMetrics = admin.getClusterMetrics();
            Map<ServerName, ServerMetrics> metrics =
                            clusterMetrics.getLiveServerMetrics();
            Map<String, Object> regionServers = InsertionOrderUtil.newMap();
            for (Map.Entry<ServerName, ServerMetrics> e : metrics.entrySet()) {
                ServerName server = e.getKey();
                ServerMetrics serverMetrics = e.getValue();
                regionServers.put(server.getAddress().toString(),
                                  formatMetrics(serverMetrics));
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

        try {
            this.hbase.compactTables(tableNames);
            results.put(SERVERS, ImmutableMap.of(SERVER_CLUSTER, "OK"));
        } catch (Throwable e) {
            results.put(EXCEPTION, e.toString());
        }
        return results;
    }

    private Map<String, Object> clusterInfo() {
        Map<String, Object> results = InsertionOrderUtil.newMap();
        try (Admin admin = this.hbase.hbase().getAdmin()) {
            // Cluster info
            ClusterMetrics clusterMetrics = admin.getClusterMetrics();
            results.put(CLUSTER_ID, clusterMetrics.getClusterId());
            results.put("master_name",
                        clusterMetrics.getMasterName().getAddress().toString());
            results.put("average_load", clusterMetrics.getAverageLoad());
            results.put("hbase_version", clusterMetrics.getHBaseVersion());
            results.put("region_count", clusterMetrics.getRegionCount());
            results.put("leaving_servers",
                        serversAddress(clusterMetrics.getDeadServerNames()));
            // Region servers info
            Collection<ServerName> regionServers = admin.getRegionServers();
            results.put(NODES, regionServers.size());
            results.put("region_servers", serversAddress(regionServers));
        } catch (Throwable e) {
            results.put(EXCEPTION, e.toString());
        }
        return results;
    }

    private static Map<String, Object> formatMetrics(
                                       ServerMetrics serverMetrics) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();

        Size memMax = serverMetrics.getMaxHeapSize();
        Size memUsed = serverMetrics.getUsedHeapSize();
        long memUsedBytes = (long) memUsed.get(Size.Unit.BYTE);
        metrics.put(MEM_MAX, memMax.get(Size.Unit.MEGABYTE));
        metrics.put(MEM_USED, memUsed.get(Size.Unit.MEGABYTE));
        metrics.put(MEM_USED + READABLE,
                    UnitUtil.bytesToReadableString(memUsedBytes));
        metrics.put(MEM_UNIT, "MB");

        Collection<RegionMetrics> regions = serverMetrics.getRegionMetrics()
                                                         .values();
        long fileSizeBytes = 0L;
        for (RegionMetrics region : regions) {
            fileSizeBytes += region.getStoreFileSize().get(Size.Unit.BYTE);
        }
        metrics.put(DISK_USAGE, UnitUtil.bytesToGB(fileSizeBytes));
        metrics.put(DISK_USAGE + READABLE,
                    UnitUtil.bytesToReadableString(fileSizeBytes));
        metrics.put(DISK_UNIT, "GB");

        metrics.put("request_count", serverMetrics.getRequestCount());
        metrics.put("request_count_per_second",
                    serverMetrics.getRequestCountPerSecond());
        metrics.put("coprocessor_names", serverMetrics.getCoprocessorNames());

        metrics.put("regions", formatRegions(regions));

        return metrics;
    }

    private static Map<String, Object> formatRegions(
                                       Collection<RegionMetrics> regions) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        for (RegionMetrics region : regions) {
            metrics.put(region.getNameAsString(), formatRegion(region));
        }
        return metrics;
    }

    private static Map<String, Object> formatRegion(RegionMetrics region) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();

        Size fileSize = region.getStoreFileSize();
        long fileSizeBytes = (long) fileSize.get(Size.Unit.BYTE);
        metrics.put(DISK_USAGE, fileSize.get(Size.Unit.GIGABYTE));
        metrics.put(DISK_USAGE + READABLE,
                    UnitUtil.bytesToReadableString(fileSizeBytes));
        metrics.put(DISK_UNIT, "GB");

        metrics.put("index_store_size",
                    region.getStoreFileIndexSize().get(Size.Unit.MEGABYTE));
        metrics.put("root_level_index_store_size",
                    region.getStoreFileRootLevelIndexSize()
                          .get(Size.Unit.MEGABYTE));
        metrics.put("mem_store_size",
                    region.getMemStoreSize().get(Size.Unit.MEGABYTE));
        metrics.put("bloom_filter_size",
                    region.getBloomFilterSize().get(Size.Unit.MEGABYTE));
        metrics.put("size_unit", "MB");

        metrics.put("store_count", region.getStoreCount());
        metrics.put("store_file_count", region.getStoreFileCount());

        metrics.put("request_count", region.getRequestCount());
        metrics.put("write_request_count", region.getWriteRequestCount());
        metrics.put("read_request_count", region.getReadRequestCount());
        metrics.put("filtered_read_request_count",
                    region.getFilteredReadRequestCount());

        metrics.put("completed_sequence_id", region.getCompletedSequenceId());
        metrics.put("data_locality", region.getDataLocality());
        metrics.put("compacted_cell_count", region.getCompactedCellCount());
        metrics.put("compacting_cell_count", region.getCompactingCellCount());
        metrics.put("last_compaction_time",
                    new Date(region.getLastMajorCompactionTimestamp()));

        return metrics;
    }

    private static List<String> serversAddress(
                                Collection<ServerName> servers) {
        return servers.stream().map(server -> {
            return server.getAddress().toString();
        }).collect(Collectors.toList());
    }
}
