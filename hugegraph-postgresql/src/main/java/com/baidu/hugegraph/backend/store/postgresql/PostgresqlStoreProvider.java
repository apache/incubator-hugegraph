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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions;
import com.baidu.hugegraph.backend.store.mysql.MysqlStoreProvider;
import com.baidu.hugegraph.backend.store.mysql.MysqlTable;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;

public class PostgresqlStoreProvider extends MysqlStoreProvider {

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new PostgresqlSchemaStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new PostgresqlGraphStore(this, this.database(), store);
    }

    @Override
    public String type() {
        return "postgresql";
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] #441: supports PostgreSQL and Cockroach backend
         * [1.1] #270 & #398: support shard-index and vertex + sortkey prefix
         */
        return "1.1";
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
            this.checkSessionConnected();
            MysqlSessions.Session session = this.session(type);
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            this.checkSessionConnected();
            MysqlSessions.Session session = this.session(type);
            return this.counters.getCounter(session, type);
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
            registerTableManager(HugeType.RANGE_INDEX,
                                 new PostgresqlTables.RangeIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new PostgresqlTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new PostgresqlTables.ShardIndex(store));
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
}
