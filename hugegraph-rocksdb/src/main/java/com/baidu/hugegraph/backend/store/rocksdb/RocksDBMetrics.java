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

package com.baidu.hugegraph.backend.store.rocksdb;

import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.UnitUtil;
import com.google.common.collect.ImmutableMap;

public class RocksDBMetrics implements BackendMetrics {

    // https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h#L722
    private static final String PREFIX = "rocksdb.";

    // memory
    private static final String BLOCK_CACHE = PREFIX +
                                "block-cache-usage";
    private static final String BLOCK_CACHE_PINNED = PREFIX +
                                "block-cache-pinned-usage";
    private static final String BLOCK_CACHE_CAPACITY = PREFIX +
                                "block-cache-capacity";
    private static final String INDEX_FILTER = PREFIX +
                                "estimate-table-readers-mem";
    private static final String ALL_MEM_TABLE = PREFIX +
                                "size-all-mem-tables";
    private static final String CUR_MEM_TABLE = PREFIX +
                                "cur-size-all-mem-tables";
    // disk
    private static final String DISK_USAGE = PREFIX +
                                "disk-usage";
    private static final String LIVE_DATA_SIZE = PREFIX +
                                "estimate-live-data-size";
    private static final String SST_FILE_SIZE = PREFIX +
                                "total-sst-files-size";
    private static final String LIVE_SST_FILE_SIZE = PREFIX +
                                "live-sst-files-size";
    private static final String PENDING_COMPACTION_BYTES = PREFIX +
                                "estimate-pending-compaction-bytes";

    // count/number
    private static final String NUM_KEYS = PREFIX +
                                "estimate-num-keys";
    private static final String NUM_KEYS_MEM_TABLE = PREFIX +
                                "num-entries-active-mem-table";
    private static final String NUM_KEYS_IMM_MEM_TABLE = PREFIX +
                                "num-entries-imm-mem-tables";
    private static final String NUM_DELETES_MEM_TABLE = PREFIX +
                                "num-deletes-active-mem-table";
    private static final String NUM_DELETES_IMM_MEM_TABLE = PREFIX +
                                "num-deletes-imm-mem-tables";

    private static final String RUNNING_FLUSHS = PREFIX +
                                "num-running-flushes";
    private static final String MEM_TABLE_FLUSH_PENDINF = PREFIX +
                                "mem-table-flush-pending";
    private static final String RUNNING_COMPACTIONS = PREFIX +
                                "num-running-compactions";
    private static final String COMPACTION_PENDINF = PREFIX +
                                "compaction-pending";

    private static final String NUM_IMM_MEM_TABLE = PREFIX +
                                "num-immutable-mem-table";
    private static final String NUM_SNAPSHOTS = PREFIX +
                                "num-snapshots";
    private static final String OLDEST_SNAPSHOT_TIME = PREFIX +
                                "oldest-snapshot-time";
    private static final String NUM_LIVE_VERSIONS = PREFIX +
                                "num-live-versions";
    private static final String SUPER_VERSION = PREFIX +
                                "current-super-version-number";

    public static final String KEY_DISK_USAGE = DISK_USAGE;
    public static final String KEY_NUM_KEYS = NUM_KEYS;

    private final List<RocksDBSessions> dbs;
    private final RocksDBSessions.Session session;

    public RocksDBMetrics(List<RocksDBSessions> dbs,
                          RocksDBSessions.Session session) {
        this.dbs = dbs;
        this.session = session;
    }

    @Override
    public Map<String, Object> metrics() {
        Map<String, Object> results = InsertionOrderUtil.newMap();
        results.put(NODES, 1);
        results.put(CLUSTER_ID, SERVER_LOCAL);
        try {
            Map<String, Object> metrics = metricsInfo();
            results.put(SERVERS, ImmutableMap.of(SERVER_LOCAL, metrics));
        } catch (Throwable e) {
            results.put(EXCEPTION, e.toString());
        }
        return results;
    }

    private Map<String, Object> metricsInfo() {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        metrics.put(MEM_UNIT, "MB");
        metrics.put(DISK_UNIT, "GB");

        // NOTE: the unit of rocksdb memory property is bytes
        long memUsed = (long) this.getMemUsed();
        metrics.put(MEM_USED, UnitUtil.bytesToMB(memUsed));
        metrics.put(MEM_USED + "_readable",
                    UnitUtil.bytesToReadableString(memUsed));

        long diskUsage = (long) this.getDiskUsage();
        metrics.put(DISK_USAGE, UnitUtil.bytesToMB(diskUsage));
        metrics.put(DISK_USAGE + "_readable",
                    UnitUtil.bytesToReadableString(diskUsage));

        // memory
        this.appendMetricsMemory(metrics, BLOCK_CACHE);
        this.appendMetricsMemory(metrics, BLOCK_CACHE_PINNED);
        this.appendMetricsMemory(metrics, BLOCK_CACHE_CAPACITY);
        this.appendMetricsMemory(metrics, INDEX_FILTER);
        this.appendMetricsMemory(metrics, ALL_MEM_TABLE);
        this.appendMetricsMemory(metrics, CUR_MEM_TABLE);

        // disk
        this.appendMetricsDisk(metrics, LIVE_DATA_SIZE);
        this.appendMetricsDisk(metrics, SST_FILE_SIZE);
        this.appendMetricsDisk(metrics, LIVE_SST_FILE_SIZE);
        this.appendMetricsDisk(metrics, PENDING_COMPACTION_BYTES);

        // count/number
        this.appendMetricsNumber(metrics, NUM_KEYS);
        this.appendMetricsNumber(metrics, NUM_KEYS_MEM_TABLE);
        this.appendMetricsNumber(metrics, NUM_KEYS_IMM_MEM_TABLE);
        this.appendMetricsNumber(metrics, NUM_DELETES_MEM_TABLE);
        this.appendMetricsNumber(metrics, NUM_DELETES_IMM_MEM_TABLE);
        this.appendMetricsNumber(metrics, RUNNING_FLUSHS);
        this.appendMetricsNumber(metrics, MEM_TABLE_FLUSH_PENDINF);
        this.appendMetricsNumber(metrics, RUNNING_COMPACTIONS);
        this.appendMetricsNumber(metrics, COMPACTION_PENDINF);
        this.appendMetricsNumber(metrics, NUM_IMM_MEM_TABLE);
        this.appendMetricsNumber(metrics, NUM_SNAPSHOTS);
        this.appendMetricsNumber(metrics, OLDEST_SNAPSHOT_TIME);
        this.appendMetricsNumber(metrics, NUM_LIVE_VERSIONS);
        this.appendMetricsNumber(metrics, SUPER_VERSION);

        return metrics;
    }

    private void appendMetricsMemory(Map<String, Object> metrics, String key) {
        metrics.put(name(key), this.sum(this.session, key) / Bytes.MB);
    }

    private void appendMetricsDisk(Map<String, Object> metrics, String key) {
        metrics.put(name(key), this.sum(this.session, key) / Bytes.GB);
    }

    private void appendMetricsNumber(Map<String, Object> metrics, String key) {
        metrics.put(name(key), (long) this.sum(this.session, key));
    }

    private String name(String key) {
        return key.replace(PREFIX, "").replace("-", "_");
    }

    private double getMemUsed() {
        // https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
        double blockCache = this.sum(this.session, BLOCK_CACHE);
        double indexFilter = this.sum(this.session, INDEX_FILTER);
        double memtable = this.sum(this.session, ALL_MEM_TABLE);
        double blockCachePinned = this.sum(this.session, BLOCK_CACHE_PINNED);
        return blockCache + indexFilter + memtable + blockCachePinned;
    }

    private double getDiskUsage() {
        return this.sum(DISK_USAGE);
    }

    private double sum(RocksDBSessions.Session session, String property) {
        double total = 0;
        for (RocksDBSessions db : this.dbs) {
            List<String> cfValues = db.property(property);
            for(String value : cfValues) {
                total += Double.parseDouble(value);
            }
            for (String table : db.openedTables()) {
                total += Double.parseDouble(session.property(table, property));
            }
        }
        return total;
    }

    private double sum(String property) {
        double total = 0;
        for (RocksDBSessions db : this.dbs) {
            List<String> cfValues = db.property(property);
            for(String value : cfValues) {
                total += Double.parseDouble(value);
            }
        }
        return total;
    }

    public Map<String, Object> compact() {
        Map<String, Object> results = InsertionOrderUtil.newMap();
        results.put(NODES, 1);
        results.put(CLUSTER_ID, SERVER_LOCAL);

        try {
            for (RocksDBSessions db : this.dbs) {
                // NOTE: maybe cost long time
                db.compactRange();
            }
            results.put(SERVERS, ImmutableMap.of(SERVER_LOCAL, "OK"));
        } catch (Throwable e) {
            results.put(EXCEPTION, e.toString());
        }

        return results;
    }
}
