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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.MutateItem;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public abstract class RocksDBStore implements BackendStore {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private static final BackendFeatures FEATURES = new RocksDBFeatures();

    private final String name;
    private final String database;

    private final BackendStoreProvider provider;
    private final Map<HugeType, RocksDBTable> tables;

    private HugeConfig conf;

    private RocksDBSessions sessions;

    private static Map<String, RocksDBSessions> dbs = new ConcurrentHashMap<>();

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

    protected List<String> tableNames() {
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
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.name);

        E.checkNotNull(config, "config");
        this.conf = config;

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.name);
            this.sessions.useSession();
            return;
        }

        String data = wrapPath(this.conf.get(RocksDBOptions.DATA_PATH));
        String wal = wrapPath(this.conf.get(RocksDBOptions.WAL_PATH));
        LOG.info("Opening RocksDB with data path: {}", data);

        try {
            this.sessions = newSessions(this.conf, data, wal, tableNames());
        } catch (RocksDBException e) {
            if (dbs.containsKey(data)) {
                if (e.getMessage().contains("No locks available")) {
                    // Open twice, but we should support keyspace
                    this.sessions = dbs.get(data);
                } else if (e.getMessage().contains("Column family not found")) {
                    // Open a keyspace after other keyspace closed
                    try {
                        // Will open old CFs(of other keyspace)
                        final List<String> none = ImmutableList.of();
                        this.sessions = newSessions(this.conf, data, wal, none);
                    } catch (RocksDBException e1) {
                        // Let it throw later
                        e = e1;
                    }
                }
            } else if (e.getMessage().contains("Column family not found")) {
                // Before init the first keyspace
                LOG.info("Failed to open RocksDB '{}' with database '{}', " +
                         "try to init CF later", this.name, this.database);
                try {
                    // Only open default CF, won't open old CFs
                    this.sessions = newSessions(this.conf, data, wal, null);
                } catch (RocksDBException e1) {
                    LOG.error("Failed to open RocksDB with default CF", e1);
                }
            }

            if (this.sessions == null) {
                // Error after trying other ways
                LOG.error("Failed to open RocksDB '{}'", this.name, e);
                throw new BackendException("Failed to open RocksDB '%s'",
                                           e, this.name);
            }
        }

        if (this.sessions != null) {
            dbs.put(data, this.sessions);
            this.sessions.session();
            LOG.debug("Store opened: {}", this.name);
        }
    }

    protected RocksDBSessions newSessions(HugeConfig config,
                                          String data, String wal,
                                          List<String> tableNames)
                                          throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBStdSessions(config, data, wal);
        } else {
            return new RocksDBStdSessions(config, data, wal, tableNames);
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

        Session session = this.session();
        for (List<MutateItem> items : mutation.mutation().values()) {
            for (MutateItem item : items) {
               this.mutate(session, item);
            }
        }
    }

    private void mutate(Session session, MutateItem item) {
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
        RocksDBTable table = this.table(query.resultType());
        return table.query(this.session(), query);
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
        Session session = this.session();

        Object count = session.commit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} committed {} items", this.name, count);
        }
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();
        Session session = this.session();

        session.clear();
    }

    @Override
    public Object metadata(HugeType type, String meta, Object[] args) {
        throw new UnsupportedOperationException("RocksDBStore.metadata()");
    }

    private Session session() {
        this.checkOpened();
        return this.sessions.session();
    }

    private void checkOpened() {
        E.checkState(this.sessions != null && !this.sessions.closed(),
                     "RocksDB store has not been opened");
    }

    private String wrapPath(String path) {
        // Ensure the `path` exists
        try {
            FileUtils.forceMkdir(FileUtils.getFile(path));
        } catch (IOException e) {
            throw new BackendException(e.getMessage(), e);
        }
        // Join with store type
        return Paths.get(path, this.name).toString();
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSchemaStore extends RocksDBStore {

        private final RocksDBTables.Counters counters;

        public RocksDBSchemaStore(BackendStoreProvider provider,
                                  String database, String name) {
            super(provider, database, name);

            this.counters = new RocksDBTables.Counters(database);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new RocksDBTables.VertexLabel(database));
            registerTableManager(HugeType.EDGE_LABEL,
                                 new RocksDBTables.EdgeLabel(database));
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new RocksDBTables.PropertyKey(database));
            registerTableManager(HugeType.INDEX_LABEL,
                                 new RocksDBTables.IndexLabel(database));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.counters.table());
            return tableNames;
        }

        @Override
        public Id nextId(HugeType type) {
            super.checkOpened();
            Session session = super.sessions.session();
            return this.counters.nextId(session, type);
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
            registerTableManager(HugeType.RANGE_INDEX,
                                 new RocksDBTables.RangeIndex(database));
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "RocksDBGraphStore.nextId()");
        }
    }
}
