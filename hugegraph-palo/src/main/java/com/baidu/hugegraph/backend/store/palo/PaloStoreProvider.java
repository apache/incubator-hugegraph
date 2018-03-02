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

package com.baidu.hugegraph.backend.store.palo;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.LocalCounter;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore;
import com.baidu.hugegraph.backend.store.mysql.MysqlStoreProvider;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class PaloStoreProvider extends MysqlStoreProvider {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    private static final BackendFeatures FEATURES = new PaloFeatures();

    @Override
    public String type() {
        return "palo";
    }

    @Override
    public BackendStore loadSchemaStore(String name) {
        LOG.debug("PaloStoreProvider load SchemaStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new PaloSchemaStore(this, this.database(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof PaloSchemaStore,
                     "SchemaStore must be an instance of PaloSchemaStore");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.debug("PaloStoreProvider load GraphStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            BackendStore s = new PaloGraphStore(this, this.database(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof PaloGraphStore,
                     "GraphStore must be an instance of PaloGraphStore");
        return store;
    }

    public static class PaloSchemaStore extends PaloStore {

        private final LocalCounter counter;

        public PaloSchemaStore(BackendStoreProvider provider,
                               String database, String name) {
            super(provider, database, name);

            this.counter = new LocalCounter();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new PaloTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new PaloTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new PaloTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new PaloTables.IndexLabel());
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }

        @Override
        public Id nextId(HugeType type) {
            return this.counter.nextId(type);
        }
    }

    public static class PaloGraphStore extends PaloStore {

        public PaloGraphStore(BackendStoreProvider provider,
                              String database, String name) {
            super(provider, database, name);

            registerTableManager(HugeType.VERTEX,
                                 new PaloTables.Vertex());
            registerTableManager(HugeType.EDGE_OUT,
                                 new PaloTables.Edge(Directions.OUT));
            registerTableManager(HugeType.EDGE_IN,
                                 new PaloTables.Edge(Directions.IN));
            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new PaloTables.SecondaryIndex());
            registerTableManager(HugeType.RANGE_INDEX,
                                 new PaloTables.RangeIndex());
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("PaloGraphStore.nextId()");
        }
    }
}
