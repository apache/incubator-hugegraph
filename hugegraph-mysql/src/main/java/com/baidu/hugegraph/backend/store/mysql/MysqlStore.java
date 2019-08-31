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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.AbstractBackendStore;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions.Session;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class MysqlStore extends AbstractBackendStore<Session> {

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
            this.sessions.open();
        } catch (Exception e) {
            if (!e.getMessage().startsWith("Unknown database") &&
                !e.getMessage().endsWith("does not exist")) {
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

        LOG.debug("Store initialized: {}", this.store);
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

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public void truncate() {
        this.checkSessionConnected();

        this.truncateTables();
        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkSessionConnected();
        Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(Session session, BackendAction item) {
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

        Session session = this.sessions.session();
        try {
            session.begin();
        } catch (SQLException e) {
            throw new BackendException("Failed to open transaction", e);
        }
    }

    @Override
    public void commitTx() {
        this.checkSessionConnected();

        Session session = this.sessions.session();
        int count = session.commit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} committed {} items", this.store, count);
        }
    }

    @Override
    public void rollbackTx() {
        this.checkSessionConnected();
        Session session = this.sessions.session();
        session.rollback();
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    protected void initTables() {
        Session session = this.sessions.session();
        for (MysqlTable table : this.tables()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        Session session = this.sessions.session();
        for (MysqlTable table : this.tables()) {
            table.clear(session);
        }
    }

    protected void truncateTables() {
        Session session = this.sessions.session();
        for (MysqlTable table : this.tables()) {
            table.truncate(session);
        }
    }

    protected Collection<MysqlTable> tables() {
        return this.tables.values();
    }

    @Override
    protected final MysqlTable table(HugeType type) {
        assert type != null;
        MysqlTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected Session session(HugeType type) {
        this.checkSessionConnected();
        return this.sessions.session();
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
        protected Collection<MysqlTable> tables() {
            List<MysqlTable> tables = new ArrayList<>(super.tables());
            tables.add(this.counters);
            return tables;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.checkSessionConnected();
            Session session = super.sessions.session();
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            this.checkSessionConnected();
            Session session = super.sessions.session();
            return this.counters.getCounter(session, type);
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
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new MysqlTables.RangeIntIndex(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new MysqlTables.RangeFloatIndex(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new MysqlTables.RangeLongIndex(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new MysqlTables.RangeDoubleIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new MysqlTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new MysqlTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new MysqlTables.UniqueIndex(store));
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("MysqlGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "MysqlGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "MysqlGraphStore.getCounter()");
        }
    }
}
