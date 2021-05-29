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

package com.baidu.hugegraph.backend.store.scylladb;

import java.util.Map;

import org.apache.cassandra.tools.NodeProbe;

import com.baidu.hugegraph.backend.store.cassandra.CassandraMetrics;
import com.baidu.hugegraph.backend.store.cassandra.CassandraSessionPool;
import com.baidu.hugegraph.config.HugeConfig;

public class ScyllaDBMetrics extends CassandraMetrics {

    public ScyllaDBMetrics(HugeConfig conf,
                           CassandraSessionPool sessions,
                           String keyspace) {
        super(conf, sessions, keyspace);
    }

    @Override
    protected void appendExtraMetrics(Map<String, Object> metrics,
                                      NodeProbe probe) {
        // Table counter Metrics
        appendCounterMetrics(metrics, probe, this.keyspace(), this.tables(),
                             "EstimatedPartitionCount");
        appendCounterMetrics(metrics, probe, this.keyspace(), this.tables(),
                             "DroppedMutations");
        appendCounterMetrics(metrics, probe, this.keyspace(), this.tables(),
                             "PendingFlushes");
        //appendCounterMetrics(metrics, probe, this.keyspace(), this.tables(),
        //                     "KeyCacheHitRate");
        appendCounterMetrics(metrics, probe, this.keyspace(), this.tables(),
                             "BloomFilterFalseRatio");

        //Table timer Metrics
        //appendTimerMetrics(metrics, probe, this.keyspace(), "WriteLatency");
        //appendTimerMetrics(metrics, probe, this.keyspace(), "ReadLatency");
        //appendTimerMetrics(metrics, probe, null, "WriteLatency");
        //appendTimerMetrics(metrics, probe, null, "ReadLatency");

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
        //appendCompactionMetrics(metrics, probe, "BytesCompacted");
    }
}
