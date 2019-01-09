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

import org.apache.commons.io.FileUtils;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class RocksDBMetrics implements BackendMetrics {

    public static final String BLOCK_CACHE = "rocksdb.block-cache-usage";
    public static final String INDEX_FILTER =
                               "rocksdb.estimate-table-readers-mem";
    public static final String MEM_TABLE = "rocksdb.cur-size-all-mem-tables";

    public static final String DISK_USAGE = "rocksdb.disk-usage";

    private final List<RocksDBSessions> dbs;
    private final RocksDBSessions.Session session;

    public RocksDBMetrics(List<RocksDBSessions> dbs,
                          RocksDBSessions.Session session) {
        this.dbs = dbs;
        this.session = session;
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        // NOTE: the unit of rocksdb mem property is bytes
        metrics.put(MEM_USED, this.getMemUsed() / Bytes.MB);
        metrics.put(MEM_UNIT, "MB");
        String size = FileUtils.byteCountToDisplaySize(this.getDataSize());
        metrics.put(DATA_SIZE, size);
        return metrics;
    }

    private double getMemUsed() {
        double blockCache = this.sum(this.session, BLOCK_CACHE);
        double indexFilter = this.sum(this.session, INDEX_FILTER);
        double memtable = this.sum(this.session, MEM_TABLE);
        return blockCache + indexFilter + memtable;
    }

    private long getDataSize() {
        return (long) this.sum(DISK_USAGE);
    }

    private double sum(RocksDBSessions.Session session, String property) {
        double total = 0;
        for (RocksDBSessions db : this.dbs) {
            total += Double.parseDouble(db.property(property));
            for (String table : db.openedTables()) {
                total += Double.parseDouble(session.property(table, property));
            }
        }
        return total;
    }

    private double sum(String property) {
        double total = 0;
        for (RocksDBSessions db : this.dbs) {
            total += Double.parseDouble(db.property(property));
        }
        return total;
    }
}
