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

import org.apache.cassandra.tools.NodeProbe;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;

public class CassandraMetrics implements BackendMetrics {

    private final Cluster cluster;
    private final int port;
    private final String username;
    private final String password;

    public CassandraMetrics(CassandraSessionPool sessions, HugeConfig conf) {
        E.checkArgumentNotNull(sessions,
                               "Cassandra sessions have not been initialized");
        this.cluster = sessions.cluster();
        this.port = conf.get(CassandraOptions.CASSANDRA_JMX_PORT);
        this.username = conf.get(CassandraOptions.CASSANDRA_USERNAME);
        this.password = conf.get(CassandraOptions.CASSANDRA_PASSWORD);
        assert this.username != null && this.password != null;
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> results = InsertionOrderUtil.newMap();
        Set<Host> hosts = this.cluster.getMetadata().getAllHosts();
        results.put(NODES, hosts.size());
        for (Host host : hosts) {
            String address = host.getAddress().getHostAddress();
            results.put(address, this.getMetricsByHost(address));
        }
        return results;
    }

    private Map<String, Object> getMetricsByHost(String host) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        // JMX client operations for Cassandra.
        try (NodeProbe probe = this.newNodeProbe(host)) {
            MemoryUsage heapUsage = probe.getHeapMemoryUsage();
            metrics.put(MEM_USED, heapUsage.getUsed() / Bytes.MB);
            metrics.put(MEM_COMMITED, heapUsage.getCommitted() / Bytes.MB);
            metrics.put(MEM_MAX, heapUsage.getMax() / Bytes.MB);
            metrics.put(MEM_UNIT, "MB");
            metrics.put(DATA_SIZE, probe.getLoadString());
        } catch (Throwable e) {
            metrics.put(EXCEPTION, e.toString());
        }
        return metrics;
    }

    private NodeProbe newNodeProbe(String host) throws IOException {
        return this.username.isEmpty() ?
               new NodeProbe(host, this.port) :
               new NodeProbe(host, this.port, this.username, this.password);
    }
}
