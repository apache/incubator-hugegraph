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

import java.util.List;

import org.rocksdb.RocksDBException;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBStore;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBTables;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;

public abstract class RocksDBSstStore extends RocksDBStore {

    public RocksDBSstStore(final BackendStoreProvider provider,
                           final String database, final String name) {
        super(provider, database, name);
    }

    @Override
    protected RocksDBSessions newSessions(HugeConfig config,
                                          String data, String wal,
                                          List<String> tableNames)
                                          throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBSstSessions(config, data);
        } else {
            return new RocksDBSstSessions(config, data, tableNames);
        }
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSstGraphStore extends RocksDBSstStore {

        public RocksDBSstGraphStore(BackendStoreProvider provider,
                                    String database, String name) {
            super(provider, database, name);

            registerTableManager(HugeType.VERTEX,
                                 new RocksDBTables.Vertex(database));
            registerTableManager(HugeType.EDGE,
                                 new RocksDBTables.Edge(database));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
            registerTableManager(HugeType.RANGE_INDEX,
                                 new RocksDBTables.RangeIndex(database));
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "RocksDBSstGraphStore.nextId()");
        }
    }
}
