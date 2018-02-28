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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class MysqlStore implements BackendStore {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    private static final BackendFeatures FEATURES = new MysqlFeatures();

    private final String name;
    private final String database;

    private final BackendStoreProvider provider;

    private final Map<HugeType, MysqlTable> tables;

    private MysqlSessions sessions;

    public MysqlStore(final BackendStoreProvider provider,
                      final String database, final String name) {
        E.checkNotNull(database, "database");
        E.checkNotNull(name, "name");
        this.provider = provider;
        this.database = database;
        this.name = name;

        this.sessions = null;
        this.tables = new ConcurrentHashMap<>();

        LOG.debug("Store loaded: {}", name);
    }

    protected void registerTableManager(HugeType type, MysqlTable table) {
        this.tables.put(type, table);
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
        LOG.debug("Store open: {}", this.name);

        E.checkNotNull(config, "config");

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.name);
            this.sessions.useSession();
            return;
        }

        this.sessions = new MysqlSessions(config, this.database);

        try {
            LOG.debug("Store connect with database: {}", this.database);
            try {
                this.sessions.open();
            } catch (SQLException e) {
                if (!e.getMessage().contains(String.format(
                    "Unknown database '%s'", this.database))) {
                    throw e;
                }
                LOG.info("Failed to open database '{}', " +
                         "try to init database later", this.database);
            }
        } catch (Throwable e) {
            try {
                this.sessions.close();
            } catch (Throwable e2) {
                LOG.warn("Failed to close connection after an error", e2);
            }
            throw new BackendException("Failed to open connection", e);
        }

        LOG.debug("Store opened: {}", this.name);
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.name);
        this.checkClusterConnected();
        this.sessions.close();
    }

    @Override
    public void init() {
        this.checkClusterConnected();
        this.sessions.createDatabase();

        // Open a new session connected with database
        try {
            this.sessions.session().open();
        } catch (SQLException e) {
            throw new BackendException("Failed to connect database '%s'",
                                       this.database);
        }

        this.checkSessionConnected();
        this.initTables();

        LOG.info("Store initialized: {}", this.name);
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

        LOG.info("Store cleared: {}", this.name);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.name, mutation);
        }

        this.checkSessionConnected();
        MysqlSessions.Session session = this.sessions.session();

        for (List<MutateItem> items : mutation.mutation().values()) {
            for (MutateItem item : items) {
                this.mutate(session, item);
            }
        }
    }

    private void mutate(MysqlSessions.Session session, MutateItem item) {
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
                          "Unsupported mutate type: %s", item.action()));
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
            LOG.debug("Store {} committed {} items", this.name, count);
        }
    }

    @Override
    public void rollbackTx() {
        this.checkSessionConnected();
        MysqlSessions.Session session = this.sessions.session();
        session.rollback();
    }

    @Override
    public Object metadata(HugeType type, String meta, Object[] args) {
        throw new UnsupportedOperationException("MysqlStore.metadata()");
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public String toString() {
        return this.name;
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
                                String database, String name) {
            super(provider, database, name);

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
                               String database, String name) {
            super(provider, database, name);

            registerTableManager(HugeType.VERTEX,
                                 new MysqlTables.Vertex());

            registerTableManager(HugeType.EDGE_OUT,
                                 new MysqlTables.Edge(Directions.OUT));
            registerTableManager(HugeType.EDGE_IN,
                                 new MysqlTables.Edge(Directions.IN));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new MysqlTables.SecondaryIndex());
            registerTableManager(HugeType.RANGE_INDEX,
                                 new MysqlTables.RangeIndex());
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("MysqlGraphStore.nextId()");
        }
    }
}
