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

package com.baidu.hugegraph.backend.store.rocksdbsst;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBStoreProvider;
import com.baidu.hugegraph.backend.store.rocksdbsst.RocksDBSstStore.RocksDBSstGraphStore;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class RocksDBSstStoreProvider extends RocksDBStoreProvider {

    private static final Logger LOG = Log.logger(RocksDBSstStore.class);

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.debug("RocksDBSstStoreProvider load GraphStore '{}'", name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = new RocksDBSstGraphStore(this, database(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof RocksDBSstGraphStore,
                     "GraphStore must be an instance of RocksDBSstGraphStore");
        return store;
    }

    @Override
    public String type() {
        return "rocksdbsst";
    }
}
