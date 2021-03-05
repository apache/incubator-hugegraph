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
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.Compact;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.UnitUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;

public class CassandraMetrics implements BackendMetrics {

    private final Cluster cluster;
    private final int port;
    private final String username;
    private final String password;

    public CassandraMetrics(CassandraSessionPool sessions, HugeConfig conf) {
        E.checkNotNull(conf, "config");
        E.checkArgumentNotNull(sessions,
                               "Cassandra sessions have not been initialized");
        this.cluster = sessions.cluster();
        this.port = conf.get(CassandraOptions.CASSANDRA_JMX_PORT);
        this.username = conf.get(CassandraOptions.CASSANDRA_USERNAME);
        this.password = conf.get(CassandraOptions.CASSANDRA_PASSWORD);
        assert this.username != null && this.password != null;
    }

    @Override
    public Map<String, Object> metrics() {
        return this.executeAllHosts(this::getMetricsByHost);
    }

    private Map<String, Object> getMetricsByHost(String host) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        // JMX client operations for Cassandra.
        try (NodeProbe probe = this.newNodeProbe(host)) {
            MemoryUsage heapUsage = probe.getHeapMemoryUsage();
            metrics.put(MEM_UNIT, "MB");
            metrics.put(DISK_UNIT, "GB");
            metrics.put(MEM_USED, heapUsage.getUsed() / Bytes.MB);
            metrics.put(MEM_COMMITED, heapUsage.getCommitted() / Bytes.MB);
            metrics.put(MEM_MAX, heapUsage.getMax() / Bytes.MB);

            String diskSize = probe.getLoadString();
            metrics.put(DISK_USAGE, UnitUtil.bytesFromReadableString(diskSize));
            metrics.put("disk_usage_readable", diskSize);
            metrics.put("disk_usage_details", probe.getLoadMap());

            metrics.put("uptime", probe.getUptime());
            metrics.put("time_unit", "ms");
            metrics.put("uptime_readable",
                        UnitUtil.timestampToReadableString(probe.getUptime()));

            metrics.put("live_nodes", probe.getLiveNodes());
            metrics.put("leaving_nodes", probe.getLeavingNodes());
        } catch (Throwable e) {
            metrics.put(EXCEPTION, e.toString());
        }
        return metrics;
    }

    public Map<String, Object> compact() {
        return this.executeAllHosts(this::compactHost);
    }

    public Object compactHost(String host) {
        try (NodeProbe probe = this.newNodeProbe(host)) {
            Compact compact = new Compact();
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
