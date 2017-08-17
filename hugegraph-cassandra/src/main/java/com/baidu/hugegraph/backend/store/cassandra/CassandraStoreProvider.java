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

import org.slf4j.Logger;
import com.baidu.hugegraph.util.Log;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraGraphStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraIndexStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraSchemaStore;
import com.baidu.hugegraph.util.E;

public class CassandraStoreProvider extends AbstractBackendStoreProvider {

    private static final Logger LOG = Log.logger(CassandraStore.class);

    private String keyspace() {
        return this.name();
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        LOG.info("CassandraStoreProvider load SchemaStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new CassandraSchemaStore(this, keyspace(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof CassandraStore.CassandraSchemaStore,
                     "SchemaStore must be a instance of CassandraSchemaStore");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.info("CassandraStoreProvider load GraphStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new CassandraGraphStore(this, keyspace(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof CassandraStore.CassandraGraphStore,
                     "GraphStore must be a instance of CassandraGraphStore");
        return store;
    }

    @Override
    public BackendStore loadIndexStore(String name) {
        LOG.info("CassandraStoreProvider load IndexStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new CassandraIndexStore(this, keyspace(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof CassandraStore.CassandraIndexStore,
                     "IndexStore must be a instance of CassandraIndexStore");
        return store;
    }

    @Override
    public String type() {
        return "cassandra";
    }
}
