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

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.4] #633: support unique index
         * [1.5] #661: reduce the storage of vertex/edge id
         * [1.6] #746: support userdata for indexlabel
         * [1.7] #894: asStoredString() encoding is changed to signed B64
         *             instead of sortable B64
         * [1.8] #295: support ttl for vertex and edge
         * [1.9] #1333: support read frequency for property key
         * [1.10] #1506: rename read frequency to write type
         */
        return "1.9";
    }

    public static class PaloSchemaStore extends PaloStore {

        private final LocalCounter counter;

        public PaloSchemaStore(BackendStoreProvider provider,
                               String database, String store) {
            super(provider, database, store);

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

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.counter.increaseCounter(type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            return this.counter.getCounter(type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class PaloGraphStore extends PaloStore {

        public PaloGraphStore(BackendStoreProvider provider,
                              String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                                 new PaloTables.Vertex(store));
            registerTableManager(HugeType.EDGE_OUT,
                                 new PaloTables.Edge(store, Directions.OUT));
            registerTableManager(HugeType.EDGE_IN,
                                 new PaloTables.Edge(store, Directions.IN));
            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new PaloTables.SecondaryIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new PaloTables.RangeIntIndex(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new PaloTables.RangeFloatIndex(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new PaloTables.RangeLongIndex(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new PaloTables.RangeDoubleIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new PaloTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new PaloTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new PaloTables.UniqueIndex(store));
        }

        @Override
        public BackendFeatures features() {
            return FEATURES;
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("PaloGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "PaloGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "PaloGraphStore.getCounter()");
        }
    }
}
