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

package com.baidu.hugegraph.backend.store.postgresql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions;
import com.baidu.hugegraph.backend.store.mysql.MysqlStoreProvider;
import com.baidu.hugegraph.backend.store.mysql.MysqlTable;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.Log;

public class PostgresqlStoreProvider extends MysqlStoreProvider {

    private static final Logger LOG = Log.logger(PostgresqlStore.class);

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new PostgresqlSchemaStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new PostgresqlGraphStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newSystemStore(String store) {
        return new PostgresqlSystemStore(this, this.database(), store);
    }

    @Override
    public String type() {
        return "postgresql";
    }

    @Override
    public String driverVersion() {
        /*
         * Versions history:
         * [1.0] #441: supports PostgreSQL and Cockroach backend
         * [1.1] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.2] #633: support unique index
         * [1.3] #661: reduce the storage of vertex/edge id
         * [1.4] #691: support aggregate property
         * [1.5] #746: support userdata for indexlabel
         * [1.6] #894: asStoredString() encoding is changed to signed B64
         *             instead of sortable B64
         * [1.7] #295: support ttl for vertex and edge
         * [1.8] #1333: support read frequency for property key
         * [1.9] #1533: add meta table in system store
         */
        return "1.9";
    }

    public static class PostgresqlSchemaStore extends PostgresqlStore {

        private final PostgresqlTables.Counters counters;

        public PostgresqlSchemaStore(BackendStoreProvider provider,
                                     String database, String store) {
            super(provider, database, store);

            this.counters = new PostgresqlTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new PostgresqlTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new PostgresqlTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new PostgresqlTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new PostgresqlTables.IndexLabel());
        }

        @Override
        protected Collection<MysqlTable> tables() {
            List<MysqlTable> tables = new ArrayList<>(super.tables());
            tables.add(this.counters);
            return tables;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.checkOpened();
            MysqlSessions.Session session = this.session(type);
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            this.checkOpened();
            MysqlSessions.Session session = this.session(type);
            return this.counters.getCounter(session, type);
        }

        @Override
        public String storedVersion() {
            throw new UnsupportedOperationException(
                      "PostgresqlSchemaStore.storedVersion()");
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class PostgresqlGraphStore extends PostgresqlStore {

        public PostgresqlGraphStore(BackendStoreProvider provider,
                                    String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                                 new PostgresqlTables.Vertex(store));
            registerTableManager(HugeType.EDGE_OUT,
                                 new PostgresqlTables.Edge(store,
                                                           Directions.OUT));
            registerTableManager(HugeType.EDGE_IN,
                                 new PostgresqlTables.Edge(store,
                                                           Directions.IN));
            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new PostgresqlTables.SecondaryIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new PostgresqlTables.RangeIntIndex(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new PostgresqlTables.RangeFloatIndex(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new PostgresqlTables.RangeLongIndex(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new PostgresqlTables.RangeDoubleIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new PostgresqlTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new PostgresqlTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new PostgresqlTables.UniqueIndex(store));
        }

        @Override
        public String storedVersion() {
            throw new UnsupportedOperationException(
                      "PostgresqlGraphStore.storedVersion()");
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "PostgresqlGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            throw new UnsupportedOperationException(
                      "PostgresqlGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "PostgresqlGraphStore.getCounter()");
        }
    }

    public static class PostgresqlSystemStore extends PostgresqlGraphStore {

        private final PostgresqlTables.Meta meta;

        public PostgresqlSystemStore(BackendStoreProvider provider,
                                     String database, String store) {
            super(provider, database, store);

            this.meta = new PostgresqlTables.Meta();
        }

        @Override
        public void init() {
            super.init();
            this.checkOpened();
            MysqlSessions.Session session = this.session(HugeType.META);
            String driverVersion = this.provider().driverVersion();
            this.meta.writeVersion(session, driverVersion);
            LOG.info("Write down the backend version: {}", driverVersion);
        }

        @Override
        public String storedVersion() {
            super.init();
            this.checkOpened();
            MysqlSessions.Session session = this.session(HugeType.META);
            return this.meta.readVersion(session);
        }

        @Override
        protected Collection<MysqlTable> tables() {
            List<MysqlTable> tables = new ArrayList<>(super.tables());
            tables.add(this.meta);
            return tables;
        }
    }
}
