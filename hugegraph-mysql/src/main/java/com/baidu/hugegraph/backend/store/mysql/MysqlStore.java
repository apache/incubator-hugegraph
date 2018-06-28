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

package com.baidu.hugegraph.backend.store.mysql;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class MysqlStore implements BackendStore {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    private static final BackendFeatures FEATURES = new MysqlFeatures();

    private final String store;
    private final String database;

    private final BackendStoreProvider provider;

    private final Map<HugeType, MysqlTable> tables;

    private MysqlSessions sessions;

    public MysqlStore(final BackendStoreProvider provider,
                      final String database, final String store) {
        E.checkNotNull(database, "database");
        E.checkNotNull(store, "store");
        this.provider = provider;
        this.database = database;
        this.store = store;

        this.sessions = null;
        this.tables = new ConcurrentHashMap<>();

        LOG.debug("Store loaded: {}", store);
    }

    protected void registerTableManager(HugeType type, MysqlTable table) {
        this.tables.put(type, table);
    }

    protected MysqlSessions openSessionPool(HugeConfig config) {
        return new MysqlSessions(config, this.database, this.store);
    }

    public Map<HugeType, MysqlTable> tables() {
        return this.tables;
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.store);

        E.checkNotNull(config, "config");

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        this.sessions = this.openSessionPool(config);

        LOG.debug("Store connect with database: {}", this.database);
        try {
            this.sessions.open(config);
        } catch (Exception e) {
            if (!e.getMessage().startsWith("Unknown database")) {
                throw new BackendException("Failed connect with mysql, " +
                                           "please ensure it's ok", e);
            }
            LOG.info("Failed to open database '{}', " +
                     "try to init database later", this.database);
        }

        try {
            this.sessions.session();
        } catch (Throwable e) {
            try {
                this.sessions.close();
            } catch (Throwable e2) {
                LOG.warn("Failed to close connection after an error", e2);
            }
            throw new BackendException("Failed to open database", e);
        }

        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);
        this.checkClusterConnected();
        this.sessions.close();
    }

    @Override
    public void init() {
        this.checkClusterConnected();
        this.sessions.createDatabase();
        try {
            // Open a new session connected with specified database
            this.sessions.session().open();
        } catch (SQLException e) {
            throw new BackendException("Failed to connect database '%s'",
                                       this.database);
        }
        this.checkSessionConnected();
        this.initTables();

        LOG.info("Store initialized: {}", this.store);
    }

    @Override
    public void clear() {
        // Check connected
        this.checkClusterConnected();

        if (this.sessions.existsDatabase()) {
            this.checkSessionConnected();
            this.clearTables();
            this.sessions.dropDatabase();
        }

        LOG.info("Store cleared: {}", this.store);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkSessionConnected();
        MysqlSessions.Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(MysqlSessions.Session session, BackendAction item) {
        MysqlBackendEntry entry = castBackendEntry(item.entry());
        MysqlTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry.row());
                break;
            case DELETE:
                table.delete(session, entry.row());
                break;
            case APPEND:
                table.append(session, entry.row());
                break;
            case ELIMINATE:
                table.eliminate(session, entry.row());
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkSessionConnected();

        MysqlTable table = this.table(MysqlTable.tableType(query));
        return table.query(this.sessions.session(), query);
    }

    @Override
    public void beginTx() {
        this.checkSessionConnected();

        MysqlSessions.Session session = this.sessions.session();
        try {
            session.begin();
        } catch (SQLException e) {
            throw new BackendException("Failed to open transaction", e);
        }
    }

    @Override
    public void commitTx() {
        this.checkSessionConnected();

        MysqlSessions.Session session = this.sessions.session();
        int count = session.commit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} committed {} items", this.store, count);
        }
    }

    @Override
    public void rollbackTx() {
        this.checkSessionConnected();
        MysqlSessions.Session session = this.sessions.session();
        session.rollback();
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        throw new UnsupportedOperationException("MysqlStore.metadata()");
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public String toString() {
        return this.store;
    }

    protected void initTables() {
        MysqlSessions.Session session = this.sessions.session();
        for (MysqlTable table : this.tables.values()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        MysqlSessions.Session session = this.sessions.session();
        for (MysqlTable table : this.tables.values()) {
            table.clear(session);
        }
    }

    protected final MysqlTable table(HugeType type) {
        assert type != null;
        MysqlTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    protected final void checkClusterConnected() {
        E.checkState(this.sessions != null,
                     "MySQL store has not been initialized");
    }

    protected final void checkSessionConnected() {
        this.checkClusterConnected();
        this.sessions.checkSessionConnected();
    }

    protected static MysqlBackendEntry castBackendEntry(BackendEntry entry) {
        if (!(entry instanceof MysqlBackendEntry)) {
            throw new BackendException(
                      "MySQL store only supports MysqlBackendEntry");
        }
        return (MysqlBackendEntry) entry;
    }

    public static class MysqlSchemaStore extends MysqlStore {

        private final MysqlTables.Counters counters;

        public MysqlSchemaStore(BackendStoreProvider provider,
                                String database, String store) {
            super(provider, database, store);

            this.counters = new MysqlTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new MysqlTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new MysqlTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new MysqlTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new MysqlTables.IndexLabel());
        }

        @Override
        protected void initTables() {
            super.initTables();

            MysqlSessions.Session session = super.sessions.session();
            this.counters.init(session);
        }

        @Override
        protected void clearTables() {
            super.clearTables();

            MysqlSessions.Session session = super.sessions.session();
            this.counters.clear(session);
        }

        @Override
        public Id nextId(HugeType type) {
            this.checkSessionConnected();
            MysqlSessions.Session session = super.sessions.session();
            return this.counters.nextId(session, type);
        }
    }

    public static class MysqlGraphStore extends MysqlStore {

        public MysqlGraphStore(BackendStoreProvider provider,
                               String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                                 new MysqlTables.Vertex(store));

            registerTableManager(HugeType.EDGE_OUT,
                                 MysqlTables.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                                 MysqlTables.Edge.in(store));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new MysqlTables.SecondaryIndex(store));
            registerTableManager(HugeType.RANGE_INDEX,
                                 new MysqlTables.RangeIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new MysqlTables.SearchIndex(store));
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("MysqlGraphStore.nextId()");
        }
    }
}
