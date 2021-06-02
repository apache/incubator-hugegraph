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

package com.baidu.hugegraph.backend.store.cassandra;

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxCounterMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxTimerMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.Compact;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTables.Edge;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTables.Vertex;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.UnitUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.google.common.collect.ImmutableList;

public class CassandraMetrics implements BackendMetrics {

    private final Cluster cluster;
    private final int port;
    private final String username;
    private final String password;

    private final String keyspace;
    private final List<String> tables;

    public CassandraMetrics(HugeConfig conf,
                            CassandraSessionPool sessions,
                            String keyspace) {
        E.checkNotNull(conf, "config");
        E.checkArgumentNotNull(sessions,
                               "Cassandra sessions have not been initialized");
        this.cluster = sessions.cluster();
        this.port = conf.get(CassandraOptions.CASSANDRA_JMX_PORT);
        this.username = conf.get(CassandraOptions.CASSANDRA_USERNAME);
        this.password = conf.get(CassandraOptions.CASSANDRA_PASSWORD);
        assert this.username != null && this.password != null;

        this.keyspace = keyspace;
        String g = conf.get(CoreOptions.STORE_GRAPH);
        String v = BackendTable.joinTableName(g, Vertex.TABLE);
        String oe = BackendTable.joinTableName(g, "o" + Edge.TABLE_SUFFIX);
        String ie = BackendTable.joinTableName(g, "i" + Edge.TABLE_SUFFIX);
        this.tables = ImmutableList.of(v, oe, ie);
    }

    @Override
    public Map<String, Object> metrics() {
        return this.executeAllHosts(this::getMetricsByHost);
    }

    protected String keyspace() {
        return this.keyspace;
    }

    protected List<String> tables() {
        return Collections.unmodifiableList(this.tables);
    }

    protected Map<String, Object> getMetricsByHost(String host) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        // JMX client operations for Cassandra.
        try (NodeProbe probe = this.newNodeProbe(host)) {
            MemoryUsage heapUsage = probe.getHeapMemoryUsage();
            metrics.put(MEM_MAX, UnitUtil.bytesToMB(heapUsage.getMax()));
            metrics.put(MEM_COMMITED,
                        UnitUtil.bytesToMB(heapUsage.getCommitted()));
            metrics.put(MEM_USED, UnitUtil.bytesToMB(heapUsage.getUsed()));
            metrics.put(MEM_USED + READABLE,
                        UnitUtil.bytesToReadableString(heapUsage.getUsed()));
            metrics.put(MEM_UNIT, "MB");

            long diskSize = UnitUtil.bytesFromReadableString(
                            probe.getLoadString());
            metrics.put(DISK_USAGE, UnitUtil.bytesToGB(diskSize));
            metrics.put(DISK_USAGE + READABLE,
                        UnitUtil.bytesToReadableString(diskSize));
            metrics.put(DISK_USAGE + "_details", probe.getLoadMap());
            metrics.put(DISK_UNIT, "GB");

            // Uptime Metrics
            metrics.put("uptime", probe.getUptime());
            metrics.put("uptime_readable",
                        UnitUtil.timestampToReadableString(probe.getUptime()));
            metrics.put("time_unit", "ms");

            // Table/Cache/Compaction Metrics
            this.appendExtraMetrics(metrics, probe);

            // Nodes Metrics
            metrics.put("live_nodes", probe.getLiveNodes());
            metrics.put("joining_nodes", probe.getJoiningNodes());
            metrics.put("moving_nodes", probe.getMovingNodes());
            metrics.put("leaving_nodes", probe.getLeavingNodes());
            metrics.put("unreachable_nodes", probe.getUnreachableNodes());

            // Others
            metrics.put("keyspaces", probe.getKeyspaces());
            metrics.put("num_tables", probe.getNumberOfTables());
            metrics.put("exception_count", probe.getExceptionCount());

            /*
             * TODO: add metrics:
             * probe.getAndResetGCStats()
             * probe.getCfsProxy(keyspace, cf).estimateKeys()
             * probe.takeSnapshot(snapshotName, table, options, keyspaces)
             */
        } catch (Throwable e) {
            metrics.put(EXCEPTION, e.toString());
        }
        return metrics;
    }

    protected void appendExtraMetrics(Map<String, Object> metrics,
                                      NodeProbe probe) {
        // Table counter Metrics
        appendCounterMetrics(metrics, probe, this.keyspace, this.tables,
                             "EstimatedPartitionCount");
        appendCounterMetrics(metrics, probe, this.keyspace, this.tables,
                             "DroppedMutations");
        appendCounterMetrics(metrics, probe, this.keyspace, this.tables,
                             "PendingFlushes");
        appendCounterMetrics(metrics, probe, this.keyspace, this.tables,
                             "KeyCacheHitRate");
        appendCounterMetrics(metrics, probe, this.keyspace, this.tables,
                             "BloomFilterFalseRatio");

        // Table timer Metrics
        appendTimerMetrics(metrics, probe, this.keyspace, "WriteLatency");
        appendTimerMetrics(metrics, probe, this.keyspace, "ReadLatency");
        appendTimerMetrics(metrics, probe, null, "WriteLatency");
        appendTimerMetrics(metrics, probe, null, "ReadLatency");

        // Cache Metrics
        appendCacheMetrics(metrics, probe, "KeyCache", "Size");
        appendCacheMetrics(metrics, probe, "KeyCache", "Entries");
        appendCacheMetrics(metrics, probe, "RowCache", "Size");
        appendCacheMetrics(metrics, probe, "RowCache", "Entries");
        appendCacheMetrics(metrics, probe, "CounterCache", "Size");
        appendCacheMetrics(metrics, probe, "CounterCache", "Entries");

        // Compaction Metrics
        appendCompactionMetrics(metrics, probe, "CompletedTasks");
        appendCompactionMetrics(metrics, probe, "PendingTasks");
        appendCompactionMetrics(metrics, probe, "BytesCompacted");
    }

    protected static void appendCounterMetrics(Map<String, Object> metrics,
                                               NodeProbe probe,
                                               String keyspace,
                                               List<String> tables,
                                               String metric) {
        // "EstimatedPartitionCount" => "estimated_partition_count"
        String name = humpToLine(metric);

        // Aggregation of metrics for the whole host tables
        Number number = 0;
        for (String table : tables) {
            // like: "hugegraph", "g_v", "EstimatedPartitionCount"
            Object value = probe.getColumnFamilyMetric(keyspace, table, metric);
            if (!(value instanceof Number)) {
                value = Double.parseDouble(value.toString());
            }
            number = NumberHelper.add(number, (Number) value);
        }
        metrics.put(name, number);
    }

    protected static void appendTimerMetrics(Map<String, Object> metrics,
                                             NodeProbe probe,
                                             String keyspace,
                                             String metric) {
        // "ReadLatency" => "read_latency_hugegraph"
        String suffix = keyspace == null ? "*" : keyspace;
        String name = humpToLine(metric + "_" + suffix);
        // Aggregation of metrics for the whole host if keyspace=null
        JmxTimerMBean value = (JmxTimerMBean) probe.getColumnFamilyMetric(
                              keyspace, null, metric);
        Map<String, Object> timerMap = InsertionOrderUtil.newMap();
        timerMap.put("count", value.getCount());
        timerMap.put("min", value.getMin());
        timerMap.put("mean", value.getMean());
        timerMap.put("max", value.getMax());
        timerMap.put("stddev", value.getStdDev());
        timerMap.put("p50", value.get50thPercentile());
        timerMap.put("p75", value.get75thPercentile());
        timerMap.put("p95", value.get95thPercentile());
        timerMap.put("p98", value.get98thPercentile());
        timerMap.put("p99", value.get99thPercentile());
        timerMap.put("p999", value.get999thPercentile());
        timerMap.put("duration_unit", value.getDurationUnit());
        timerMap.put("mean_rate", value.getMeanRate());
        timerMap.put("m15_rate", value.getFifteenMinuteRate());
        timerMap.put("m5_rate", value.getFiveMinuteRate());
        timerMap.put("m1_rate", value.getOneMinuteRate());
        timerMap.put("rate_unit", value.getRateUnit());

        metrics.put(name, timerMap);
    }

    protected static void appendCompactionMetrics(Map<String, Object> metrics,
                                                  NodeProbe probe,
                                                  String metric) {
        // "CompletedTasks" => "compaction_completed_tasks"
        String name = humpToLine("compaction" + metric);
        Object value = probe.getCompactionMetric(metric);
        if (value instanceof JmxCounterMBean) {
            value = ((JmxCounterMBean) value).getCount();
        }
        metrics.put(name, value);
    }

    protected static void appendCacheMetrics(Map<String, Object> metrics,
                                             NodeProbe probe,
                                             String cacheType,
                                             String metric) {
        // "RowCache" + "Size" => "row_cache_size"
        String name = humpToLine(cacheType + metric);
        metrics.put(name, probe.getCacheMetric(cacheType, metric));
    }

    private static String humpToLine(String name) {
        name = name.replaceAll("[A-Z]", "_$0").toLowerCase();
        if (!name.isEmpty() && name.charAt(0) == '_') {
            name = name.substring(1);
        }
        return name;
    }

    public Map<String, Object> compact() {
        return this.executeAllHosts(this::compactHost);
    }

    private Object compactHost(String host) {
        try (NodeProbe probe = this.newNodeProbe(host)) {
            Compact compact = new Compact();
            /*
             * Set the keyspace to be compacted
             * NOTE: use Whitebox due to no public api is provided, args format
             * is [<keyspace> <tables>...], the first arg means keyspace.
             */
            Whitebox.invoke(compact, "args", new Class<?>[]{Object.class},
                            "add", this.keyspace);
            compact.execute(probe);
            return "OK";
        } catch (Throwable e) {
            return e.toString();
        }
    }

    private Map<String, Object> executeAllHosts(Function<String, Object> func) {
        Set<Host> hosts = this.cluster.getMetadata().getAllHosts();

        Map<String, Object> results = InsertionOrderUtil.newMap();
        results.put(CLUSTER_ID, this.cluster.getClusterName());
        results.put(NODES, hosts.size());

        Map<String, Object> hostsResults = InsertionOrderUtil.newMap();
        for (Host host : hosts) {
            String address = host.getAddress().getHostAddress();
            hostsResults.put(address, func.apply(address));
        }
        results.put(SERVERS, hostsResults);

        return results;
    }

    private NodeProbe newNodeProbe(String host) throws IOException {
        return this.username.isEmpty() ?
               new NodeProbe(host, this.port) :
               new NodeProbe(host, this.port, this.username, this.password);
    }
}
