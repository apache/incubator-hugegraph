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

package com.baidu.hugegraph.backend.store.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.MutateItem;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Log;

/**
 * NOTE:
 * InMemoryDBStore support:
 * 1.query by id (include query edges by id)
 * 2.query by condition (include query edges by condition)
 * 3.remove by id
 * 4.range query
 * 5.append/subtract index data(element-id) and vertex-property
 * 6.query edge by edge-label
 * InMemoryDBStore not support currently:
 * 1.remove by id + condition
 * 2.append/subtract edge-property
 */
public class InMemoryDBStore implements BackendStore {

    private static final Logger LOG = Log.logger(InMemoryDBStore.class);

    private final BackendStoreProvider provider;
    private final String name;
    private Map<HugeType, InMemoryDBTable> tables;
    private Map<HugeType, AtomicLong> counters;

    public InMemoryDBStore(final BackendStoreProvider provider,
                           final String name) {
        this.provider = provider;
        this.name = name;
        this.tables = new HashMap<>();
        this.counters = new ConcurrentHashMap<>();
    }

    @Override
    public Object metadata(HugeType type, String meta, Object[] args) {
        throw new UnsupportedOperationException("InMemoryDBStore.metadata()");
    }

    protected void registerTableManager(HugeType type, InMemoryDBTable table) {
        this.tables.put(type, table);
    }

    protected final InMemoryDBTable table(HugeType type) {
        assert type != null;
        InMemoryDBTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    public Iterator<BackendEntry> query(final Query query) {
        InMemoryDBTable table = this.table(query.resultType());
        Iterator<BackendEntry> rs = table.query(query);
        LOG.debug("[store {}] has result({}) for query: {}",
                  this.name, rs.hasNext(), query);
        return rs;
    }

    @Override
    public void mutate(BackendMutation mutation) {
        for (List<MutateItem> items : mutation.mutation().values()) {
            for (MutateItem item : items) {
                this.mutate(item);
            }
        }
    }

    protected void mutate(MutateItem item) {
        BackendEntry e = item.entry();
        assert e instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) e;
        InMemoryDBTable table = this.table(entry.type());
        switch (item.action()) {
            case INSERT:
                LOG.debug("[store {}] add entry: {}", this.name, entry);
                table.insert(entry);
                break;
            case DELETE:
                LOG.debug("[store {}] remove id: {}", this.name, entry.id());
                table.delete(entry);
                break;
            case APPEND:
                LOG.debug("[store {}] append entry: {}", this.name, entry);
                table.append(entry);
                break;
            case ELIMINATE:
                LOG.debug("[store {}] eliminate entry: {}", this.name, entry);
                table.eliminate(entry);
                break;
            default:
                throw new BackendException("Unsupported mutate type: %s",
                                           item.action());
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public void open(HugeConfig config) {
        LOG.info("open()");
    }

    @Override
    public void close() throws BackendException {
        LOG.info("close()");
    }

    @Override
    public void init() {
        // pass
    }

    @Override
    public void clear() {
        LOG.info("clear()");
        for (InMemoryDBTable table : this.tables.values()) {
            table.clear();
        }
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        // pass
    }

    @Override
    public void rollbackTx() {
        throw new UnsupportedOperationException(
                  "Unsupported rollback operation by InMemoryDBStore");
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public Id nextId(HugeType type) {
        AtomicLong counter = this.counters.get(type);
        if (counter == null) {
            counter = new AtomicLong(1);
            this.counters.put(type, counter);
        } else {
            counter.incrementAndGet();
        }
        return IdGenerator.of(counter.longValue());
    }

    @Override
    public String toString() {
        return this.name;
    }

    /***************************** Store defines *****************************/

    public static class InMemorySchemaStore extends InMemoryDBStore {

        public InMemorySchemaStore(BackendStoreProvider provider, String name) {
            super(provider, name);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new InMemoryDBTable(HugeType.VERTEX_LABEL));
            registerTableManager(HugeType.EDGE_LABEL,
                                 new InMemoryDBTable(HugeType.EDGE_LABEL));
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new InMemoryDBTable(HugeType.PROPERTY_KEY));
            registerTableManager(HugeType.INDEX_LABEL,
                                 new InMemoryDBTable(HugeType.INDEX_LABEL));
            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new InMemoryDBTables.SecondaryIndex());
        }
    }

    public static class InMemoryGraphStore extends InMemoryDBStore {

        public InMemoryGraphStore(BackendStoreProvider provider, String name) {
            super(provider, name);

            // TODO: separate edges from vertices
            InMemoryDBTables.Vertex vertex = new InMemoryDBTables.Vertex();
            InMemoryDBTables.Edge edge = new InMemoryDBTables.Edge(vertex);
            registerTableManager(HugeType.VERTEX, vertex);
            registerTableManager(HugeType.EDGE, edge);

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new InMemoryDBTables.SecondaryIndex());
            registerTableManager(HugeType.RANGE_INDEX,
                                 new InMemoryDBTables.RangeIndex());
        }
    }

    /**
     * InMemoryDBStore features
     */
    private static final BackendFeatures FEATURES = new BackendFeatures() {

        @Override
        public boolean supportsScanToken() {
            return false;
        }

        @Override
        public boolean supportsScanKeyPrefix() {
            return false;
        }

        @Override
        public boolean supportsScanKeyRange() {
            return false;
        }

        @Override
        public boolean supportsQuerySchemaByName() {
            // Traversal all data in memory
            return true;
        }

        @Override
        public boolean supportsQueryByLabel() {
            // Traversal all data in memory
            return true;
        }

        @Override
        public boolean supportsQueryWithRangeCondition() {
            return true;
        }

        @Override
        public boolean supportsQueryWithOrderBy() {
            return false;
        }

        @Override
        public boolean supportsQueryWithContains() {
            return true;
        }

        @Override
        public boolean supportsQueryWithContainsKey() {
            return true;
        }

        @Override
        public boolean supportsDeleteEdgeByLabel() {
            return false;
        }

        @Override
        public boolean supportsUpdateEdgeProperty() {
            return false;
        }

        @Override
        public boolean supportsTransaction() {
            return false;
        }

        @Override
        public boolean supportsNumberType() {
            return false;
        }
    };
}
