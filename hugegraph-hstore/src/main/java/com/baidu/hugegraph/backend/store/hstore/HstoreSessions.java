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

package com.baidu.hugegraph.backend.store.hstore;

import java.util.Set;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.type.define.GraphMode;
import org.apache.commons.lang3.tuple.Pair;

import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.config.HugeConfig;

public abstract class HstoreSessions extends BackendSessionPool {

    public HstoreSessions(HugeConfig config, String database, String store) {
        super(config, database + "/" + store);
    }

    public abstract Set<String> openedTables();

    public abstract void createTable(String... tables);

    public abstract void dropTable(String... tables);

    public abstract boolean existsTable(String table);

    public abstract void truncateTable(String table);
    @Override
    public abstract Session session();

    /**
     * Session for Hstore
     */
    public static abstract class Session extends AbstractBackendSession {

        public static final int SCAN_ANY = 0x80;
        public static final int SCAN_PREFIX_BEGIN = 0x01;
        public static final int SCAN_PREFIX_END = 0x02;
        public static final int SCAN_GT_BEGIN = 0x04;
        public static final int SCAN_GTE_BEGIN = 0x0c;
        public static final int SCAN_LT_END = 0x10;
        public static final int SCAN_LTE_END = 0x30;
        public static final int SCAN_HASHCODE = 0x40;
        private HugeConfig conf;
        private String graphName;

        public abstract void createTable(String tableName);
        public abstract void dropTable(String tableName);
        public abstract boolean existsTable(String tableName);
        public abstract void truncateTable(String tableName);

        public abstract Pair<byte[], byte[]> keyRange(String table);

        public abstract void put(String table, byte[] ownerKey, byte[] key,
                                 byte[] value);

        public abstract void increase(String table, byte[] ownerKey,
                                      byte[] key, byte[] value);

        public abstract void delete(String table, byte[] ownerKey, byte[] key);

        public abstract void deletePrefix(String table, byte[] ownerKey,
                                          byte[] key);

        public abstract void deleteRange(String table,byte[] ownerKeyFrom,
                                         byte[] ownerKeyTo,byte[] keyFrom,
                                         byte[] keyTo);
        public abstract byte[] get(String table, byte[] key);

        public abstract byte[] get(String table, byte[] ownerKey, byte[] key);

        public abstract BackendColumnIterator scan(String table);

        public abstract BackendColumnIterator scan(String table,
                                                   byte[] ownerKey,
                                                   byte[] prefix);

        public BackendColumnIterator scan(String table,byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo,byte[] keyFrom,
                                          byte[] keyTo) {
            return this.scan(table,ownerKeyFrom,ownerKeyTo, keyFrom, keyTo,
                             SCAN_LT_END);
        }

        public abstract BackendColumnIterator scan(String table,
                                                   byte[] ownerKeyFrom,
                                                   byte[] ownerKeyTo,
                                                   byte[] keyFrom,
                                                   byte[] keyTo,
                                                   int scanType);
        public abstract BackendColumnIterator scan(String table,
                                                   byte[] ownerKeyFrom,
                                                   byte[] ownerKeyTo,
                                                   byte[] keyFrom,
                                                   byte[] keyTo,
                                                   int scanType,byte[] query);
        public abstract BackendColumnIterator scan(String table,
                                                   int codeFrom,
                                                   int codeTo,
                                                   int scanType,byte[] query);

        public static boolean matchScanType(int expected, int actual) {
            return (expected & actual) == expected;
        }
        public abstract void merge(String table, byte[] ownerKey,
                                   byte[] key, byte[] value);
        public abstract void setMode(GraphMode mode);
        public abstract void truncate() throws Exception;
        public abstract BackendColumnIterator scan(String table,
                                                   byte[] conditionQueryToByte);

        public HugeConfig getConf() {
            return conf;
        }

        public void setConf(HugeConfig conf) {
            this.conf = conf;
        }

        public String getGraphName() {
            return graphName;
        }

        public void setGraphName(String graphName) {
            this.graphName = graphName;
        }
        public abstract void beginTx();
    }

    public interface Countable {

        public long count();
    }
}
