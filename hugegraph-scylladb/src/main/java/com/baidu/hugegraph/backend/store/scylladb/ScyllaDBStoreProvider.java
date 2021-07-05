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

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.cassandra.CassandraMetrics;
import com.baidu.hugegraph.backend.store.cassandra.CassandraSessionPool;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStoreProvider;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class ScyllaDBStoreProvider extends CassandraStoreProvider {

    private static final Logger LOG = Log.logger(CassandraStore.class);

    private static final BackendFeatures FEATURES = new ScyllaDBFeatures();

    @Override
    public String type() {
        return "scylladb";
    }

    @Override
    public BackendStore loadSchemaStore() {
        String name = SCHEMA_STORE;
        LOG.debug("ScyllaDBStoreProvider load SchemaStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new ScyllaDBSchemaStore(this, keyspace(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof ScyllaDBSchemaStore,
                     "SchemaStore must be an instance of ScyllaDBSchemaStore");
        return store;
    }

    @Override
    public BackendStore loadGraphStore() {
        String name = GRAPH_STORE;
        LOG.debug("ScyllaDBStoreProvider load GraphStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new ScyllaDBGraphStore(this, keyspace(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof ScyllaDBGraphStore,
                     "GraphStore must be an instance of ScyllaDBGraphStore");
        return store;
    }

    @Override
    public BackendStore loadSystemStore() {
        String name = SYSTEM_STORE;
        LOG.debug("ScyllaDBStoreProvider load SystemStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new ScyllaDBSystemStore(this, keyspace(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof ScyllaDBSystemStore,
                     "SystemStore must be an instance of ScyllaDBSystemStore");
        return store;
    }

    public static class ScyllaDBSchemaStore
                  extends CassandraStore.CassandraSchemaStore {

        public ScyllaDBSchemaStore(BackendStoreProvider provider,
                                   String keyspace, String store) {
            super(provider, keyspace, store);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new ScyllaDBTablesWithMV.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new ScyllaDBTablesWithMV.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new ScyllaDBTablesWithMV.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new ScyllaDBTablesWithMV.IndexLabel());
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }

        @Override
        protected CassandraMetrics createMetrics(HugeConfig conf,
                                                 CassandraSessionPool sessions,
                                                 String keyspace) {
            return new ScyllaDBMetrics(conf, sessions, keyspace);
        }
    }

    public static class ScyllaDBGraphStore
                  extends CassandraStore.CassandraGraphStore {

        public ScyllaDBGraphStore(BackendStoreProvider provider,
                                  String keyspace, String store) {
            super(provider, keyspace, store);

            registerTableManager(HugeType.VERTEX,
                                 new ScyllaDBTablesWithMV.Vertex(store));
            registerTableManager(HugeType.EDGE_OUT,
                                 ScyllaDBTablesWithMV.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                                 ScyllaDBTablesWithMV.Edge.in(store));
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }

        @Override
        protected CassandraMetrics createMetrics(HugeConfig conf,
                                                 CassandraSessionPool sessions,
                                                 String keyspace) {
            return new ScyllaDBMetrics(conf, sessions, keyspace);
        }
    }

    public static class ScyllaDBSystemStore
                  extends CassandraStore.CassandraSystemStore {

        public ScyllaDBSystemStore(BackendStoreProvider provider,
                                   String keyspace, String store) {
            super(provider, keyspace, store);
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }

        @Override
        protected CassandraMetrics createMetrics(HugeConfig conf,
                                                 CassandraSessionPool sessions,
                                                 String keyspace) {
            return new ScyllaDBMetrics(conf, sessions, keyspace);
        }
    }
}
