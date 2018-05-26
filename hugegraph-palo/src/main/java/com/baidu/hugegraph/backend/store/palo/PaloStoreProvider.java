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

import com.baidu.hugegraph.backend.LocalCounter;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.mysql.MysqlStoreProvider;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;

public class PaloStoreProvider extends MysqlStoreProvider {

    private static final BackendFeatures FEATURES = new PaloFeatures();

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new PaloSchemaStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new PaloGraphStore(this, this.database(), store);
    }

    @Override
    public String type() {
        return "palo";
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
