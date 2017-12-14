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

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.MutateItem;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class RocksDBStore implements BackendStore {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private static final BackendFeatures FEATURES = new RocksDBFeatures();

    private final String name;
    private final String database;

    private final BackendStoreProvider provider;
    private final Map<HugeType, RocksDBTable> tables;

    private HugeConfig conf;

    private RocksDBSessions sessions;

    public RocksDBStore(final BackendStoreProvider provider,
                        final String database, final String name) {
        this.tables = new HashMap<>();

        this.provider = provider;
        this.database = database;
        this.name = name;
        this.conf = null;
        this.sessions = null;
    }

    protected void registerTableManager(HugeType type, RocksDBTable table) {
        this.tables.put(type, table);
    }

    protected final RocksDBTable table(HugeType type) {
        assert type != null;
        RocksDBTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    protected final List<String> tableNames() {
        return this.tables.values().stream().map(t -> t.table())
                                            .collect(Collectors.toList());
    }

    public String database() {
        return this.database;
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
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.name);

        E.checkNotNull(config, "config");
        this.conf = config;

        if (this.sessions != null) {
            LOG.debug("Store {} has been opened before", this.name);
            this.sessions.useSession();
            return;
        }

        String dataPath = this.conf.get(RocksDBOptions.ROCKS_DATA_PATH);
        dataPath = Paths.get(dataPath, this.name).toString();

        String walPath = this.conf.get(RocksDBOptions.ROCKS_WAL_PATH);
        walPath = Paths.get(walPath, this.name).toString();

        try {
            this.sessions = new RocksDBSessions(dataPath, walPath,
                                                this.tableNames());
        } catch (RocksDBException e) {
            if (!e.getMessage().contains("Column family not found")) {
                LOG.error("Failed to open RocksDB '{}'", this.name, e);
                throw new BackendException("Failed to open RocksDB '%s'",
                                           e, this.name);
            }
            LOG.info("Failed to open RocksDB '{}' with database '{}', " +
                     "try to init CF later", this.name, this.database);
            try {
                this.sessions = new RocksDBSessions(dataPath, walPath);
            } catch (RocksDBException e1) {
                LOG.error("Failed to open RocksDB with default CF", e);
            }
        }

        if (this.sessions != null) {
            LOG.debug("Store opened: {}", this.name);
        }
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.name);

        this.checkOpened();
        this.sessions.close();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.name, mutation);
        }

        this.checkOpened();
        RocksDBSessions.Session session = this.sessions.session();

        for (List<MutateItem> items : mutation.mutation().values()) {
            for (MutateItem item : items) {
               this.mutate(session, item);
            }
        }
    }

    private void mutate(RocksDBSessions.Session session, MutateItem item) {
        BackendEntry entry = item.entry();
        RocksDBTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry);
                break;
            case DELETE:
                table.delete(session, entry);
                break;
            case APPEND:
                table.append(session, entry);
                break;
            case ELIMINATE:
                table.eliminate(session, entry);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported mutate type: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();
        RocksDBSessions.Session session = this.sessions.session();
        RocksDBTable table = this.table(query.resultType());
        return table.query(session, query);
    }

    @Override
    public void init() {
        this.checkOpened();

        for (String table : this.tableNames()) {
            try {
                this.sessions.createTable(table);
            } catch (RocksDBException e) {
                throw new BackendException("Failed to create '%s' for '%s'",
                                           e, table, this.name);
            }
        }

        LOG.info("Store initialized: {}", this.name);
    }

    @Override
    public void clear() {
        this.checkOpened();

        for (String table : this.tableNames()) {
            try {
                this.sessions.dropTable(table);
            } catch (BackendException e) {
                if (e.getMessage().contains("is not opened")) {
                    continue;
                }
                throw e;
            } catch (RocksDBException e) {
                throw new BackendException("Failed to drop '%s' for '%s'",
                                           e, table, this.name);
            }
        }

        LOG.info("Store cleared: {}", this.name);
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        this.checkOpened();
        RocksDBSessions.Session session = this.sessions.session();

        Object count = session.commit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} committed {} items", this.name, count);
        }
    }

    @Override
    public void rollbackTx() {
        // pass
    }

    @Override
    public Object metadata(HugeType type, String meta, Object[] args) {
        throw new UnsupportedOperationException("RocksDBStore.metadata()");
    }

    private void checkOpened() {
        E.checkState(this.sessions != null,
                     "RocksDB store has not been initialized");
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSchemaStore extends RocksDBStore {

        public RocksDBSchemaStore(BackendStoreProvider provider,
                                  String database, String name) {
            super(provider, database, name);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new RocksDBTables.VertexLabel(database));
            registerTableManager(HugeType.EDGE_LABEL,
                                 new RocksDBTables.EdgeLabel(database));
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new RocksDBTables.PropertyKey(database));
            registerTableManager(HugeType.INDEX_LABEL,
                                 new RocksDBTables.IndexLabel(database));

            registerTableManager(HugeType.COUNTERS,
                                 new RocksDBTables.Counters(database));
        }
    }

    public static class RocksDBGraphStore extends RocksDBStore {

        public RocksDBGraphStore(BackendStoreProvider provider,
                                 String database, String name) {
            super(provider, database, name);

            registerTableManager(HugeType.VERTEX,
                                 new RocksDBTables.Vertex(database));
            registerTableManager(HugeType.EDGE,
                                 new RocksDBTables.Edge(database));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new RocksDBTables.SearchIndex(database));
        }
    }
}
