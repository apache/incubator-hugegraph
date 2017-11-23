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

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBStore.RocksDBGraphStore;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBStore.RocksDBSchemaStore;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class RocksDBStoreProvider extends AbstractBackendStoreProvider {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    protected String database() {
        return this.name().toLowerCase();
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        LOG.debug("RocksDBStoreProvider load SchemaStore '{}'", name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = new RocksDBSchemaStore(this, database(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof RocksDBSchemaStore,
                     "SchemaStore must be an instance of RocksDBSchemaStore");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.debug("RocksDBStoreProvider load GraphStore '{}'", name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = new RocksDBGraphStore(this, database(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof RocksDBGraphStore,
                     "GraphStore must be an instance of RocksDBGraphStore");
        return store;
    }

    @Override
    public String type() {
        return "rocksdb";
    }
}
