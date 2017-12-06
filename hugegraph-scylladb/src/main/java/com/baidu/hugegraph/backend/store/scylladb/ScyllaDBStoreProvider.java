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
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStoreProvider;
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
    public BackendStore loadSchemaStore(String name) {
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
    public BackendStore loadGraphStore(String name) {
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

    public static class ScyllaDBSchemaStore
                  extends CassandraStore.CassandraSchemaStore {

        public ScyllaDBSchemaStore(BackendStoreProvider provider,
                                   String keyspace, String name) {
            super(provider, keyspace, name);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new ScyllaDBTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new ScyllaDBTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new ScyllaDBTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new ScyllaDBTables.IndexLabel());
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }
    }

    public static class ScyllaDBGraphStore
                  extends CassandraStore.CassandraGraphStore {

        public ScyllaDBGraphStore(BackendStoreProvider provider,
                                  String keyspace, String name) {
            super(provider, keyspace, name);

            // TODO: read Scylla version from conf
            int version = 17;

            if (version >= 20) {
                registerTableManager(HugeType.VERTEX,
                                     new ScyllaDBTablesWithMV.Vertex());
                registerTableManager(HugeType.EDGE,
                                     new ScyllaDBTablesWithMV.Edge());
            } else {
                registerTableManager(HugeType.VERTEX,
                                     new ScyllaDBTables.Vertex());
                registerTableManager(HugeType.EDGE,
                                     new ScyllaDBTables.Edge());
            }
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }
    }
}
