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

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.MutateItem;
import com.baidu.hugegraph.config.CassandraOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

public abstract class CassandraStore implements BackendStore {

    private static final Logger LOG = Log.logger(CassandraStore.class);

    private static final BackendFeatures FEATURES = new CassandraFeatures();

    private final String name;
    private final String keyspace;

    private final BackendStoreProvider provider;

    private CassandraSessionPool sessions;
    private Map<HugeType, CassandraTable> tables;
    private HugeConfig conf;

    public CassandraStore(final BackendStoreProvider provider,
                          final String keyspace, final String name) {
        E.checkNotNull(keyspace, "keyspace");
        E.checkNotNull(name, "name");

        this.provider = provider;

        this.keyspace = keyspace;
        this.name = name;

        this.sessions = new CassandraSessionPool(this.keyspace);
        this.tables = new ConcurrentHashMap<>();

        this.conf = null;

        LOG.debug("Store loaded: {}", name);
    }

    protected void registerTableManager(HugeType type, CassandraTable table) {
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

        if (this.sessions.opened()) {
            // TODO: maybe we should throw an exception here instead of ignore
            LOG.debug("Store {} has been opened before", this.name);
            this.sessions.useSession();
            return;
        }
        this.conf = config;

        // Init cluster
        this.sessions.open(config);

        // Init a session for current thread
        try {
            LOG.debug("Store connect with keyspace: {}", this.keyspace);
            try {
                this.sessions.session().open();
            } catch (InvalidQueryException e) {
                // TODO: the error message may be changed in different versions
                if (!e.getMessage().contains(String.format(
                    "Keyspace '%s' does not exist", this.keyspace))) {
                    throw e;
                }
                LOG.info("Failed to connect keyspace: {}, " +
                         "try to init keyspace later", this.keyspace);
            }
        } catch (Throwable e) {
            try {
                this.sessions.close();
            } catch (Throwable e2) {
                LOG.warn("Failed to close cluster after an error", e2);
            }
            throw e;
        }

        LOG.debug("Store opened: {}", this.name);
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.name);
        this.sessions.close();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.name, mutation);
        }

        this.checkSessionConnected();
        CassandraSessionPool.Session session = this.sessions.session();

        for (List<MutateItem> items : mutation.mutation().values()) {
            for (MutateItem item : items) {
               this.mutate(session, item);
            }
        }
    }

    private void mutate(CassandraSessionPool.Session session, MutateItem item) {
        CassandraBackendEntry entry = castBackendEntry(item.entry());

        // Check if the entry has no change
        if (!entry.selfChanged() && entry.subRows().isEmpty()) {
            LOG.warn("The entry will be ignored due to no change: {}", entry);
        }

        switch (item.action()) {
            case INSERT:
                // Insert entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).insert(session, entry.row());
                }
                // Insert sub rows (edges)
                for (CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).insert(session, row);
                }
                break;
            case DELETE:
                // Delete entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).delete(session, entry.row());
                }
                // Delete sub rows (edges)
                for (CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).delete(session, row);
                }
                break;
            case APPEND:
                // Append entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).append(session, entry.row());
                }
                // Append sub rows (edges)
                for (CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).append(session, row);
                }
                break;
            case ELIMINATE:
                // Eliminate entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).eliminate(session, entry.row());
                }
                // Eliminate sub rows (edges)
                for (CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).eliminate(session, row);
                }
                break;
            default:
                throw new BackendException("Unsupported mutate type: %s",
                                           item.action());
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkSessionConnected();

        CassandraTable table = this.table(query.resultType());
        return table.query(this.sessions.session(), query);
    }

    @Override
    public Object metadata(HugeType type, String meta, Object[] args) {
        this.checkSessionConnected();

        CassandraTable table = this.table(type);
        return table.metadata(this.sessions.session(), meta, args);
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public void init() {
        this.checkClusterConnected();
        this.initKeyspace();

        this.checkSessionConnected();
        this.initTables();

        LOG.info("Store initialized: {}", this.name);
    }

    @Override
    public void clear() {
        this.checkClusterConnected();

        if (this.existsKeyspace()) {
            this.checkSessionConnected();
            this.clearTables();
            this.clearKeyspace();
        }

        LOG.info("Store cleared: {}", this.name);
    }

    @Override
    public void beginTx() {
        this.checkSessionConnected();

        CassandraSessionPool.Session session = this.sessions.session();
        if (session.txState() != TxState.CLEAN) {
            LOG.warn("Store {} expect state CLEAN than {} when begin()",
                     this.name, session.txState());
        }
        session.txState(TxState.BEGIN);
    }

    @Override
    public void commitTx() {
        this.checkSessionConnected();

        CassandraSessionPool.Session session = this.sessions.session();
        if (session.txState() != TxState.BEGIN) {
            LOG.warn("Store {} expect state BEGIN than {} when commit()",
                     this.name, session.txState());
        }
        session.txState(TxState.COMMITTING);

        if (!session.hasChanges()) {
            session.txState(TxState.CLEAN);
            LOG.debug("Store {} has nothing to commit", this.name);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} commit {} statements: {}", this.name,
                      session.statements().size(), session.statements());
        }

        // TODO how to implement tx perfectly?

        // Do update
        try {
            session.commit();
            session.txState(TxState.CLEAN);
        } catch (DriverException e) {
            session.txState(TxState.COMMITT_FAIL);
            LOG.error("Failed to commit statements due to:", e);
            assert session.statements().size() > 0;
            throw new BackendException(
                      "Failed to commit %s statements: '%s'...", e,
                      session.statements().size(),
                      session.statements().iterator().next());
        }
    }

    @Override
    public void rollbackTx() {
        this.checkSessionConnected();

        CassandraSessionPool.Session session = this.sessions.session();

        // TODO how to implement perfectly?

        if (session.txState() != TxState.COMMITT_FAIL &&
            session.txState() != TxState.COMMITTING &&
            session.txState() != TxState.CLEAN) {
            LOG.warn("Store {} expect state COMMITT_FAIL/COMMITTING/CLEAN " +
                     "than {} when rollback()", this.name, session.txState());
        }
        session.txState(TxState.ROLLBACKING);

        try {
            session.clear();
        } finally {
            // Assume batch commit would auto rollback
            session.txState(TxState.CLEAN);
        }
    }

    @Override
    public String toString() {
        return this.name;
    }

    protected Cluster cluster() {
        return this.sessions.cluster();
    }

    protected void initKeyspace() {
        // Replication strategy: SimpleStrategy or NetworkTopologyStrategy
        String strategy = this.conf.get(CassandraOptions.CASSANDRA_STRATEGY);

        // Replication factor
        int factor = this.conf.get(CassandraOptions.CASSANDRA_REPLICATION);

        Map<String, Object> replication = new HashMap<>();
        replication.putIfAbsent("class", strategy);
        replication.putIfAbsent("replication_factor", factor);

        Statement stmt = SchemaBuilder.createKeyspace(this.keyspace)
                         .ifNotExists().with().replication(replication);

        // Create keyspace with non-keyspace-session
        LOG.debug("Create keyspace: {}", stmt);
        Session session = this.cluster().connect();
        try {
            session.execute(stmt);
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
        this.sessions.session().open();
    }

    protected void clearKeyspace() {
        // Drop keyspace with non-keyspace-session
        Statement stmt = SchemaBuilder.dropKeyspace(this.keyspace).ifExists();
        LOG.debug("Drop keyspace: {}", stmt);

        Session session = this.cluster().connect();
        try {
            session.execute(stmt);
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
    }

    protected boolean existsKeyspace() {
        return this.cluster().getMetadata().getKeyspace(this.keyspace) != null;
    }

    protected void initTables() {
        CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables.values()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables.values()) {
            table.clear(session);
        }
    }

    protected final CassandraTable table(HugeType type) {
        assert type != null;
        CassandraTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    protected final void checkClusterConnected() {
        E.checkState(this.sessions != null,
                     "Cassandra store has not been initialized");
        this.sessions.checkClusterConnected();
    }

    protected final void checkSessionConnected() {
        E.checkState(this.sessions != null,
                     "Cassandra store has not been initialized");
        try {
            this.sessions.checkSessionConnected();
        } catch (DriverException e) {
            throw new BackendException("Can't connect to cassandra", e);
        }
    }

    protected static final CassandraBackendEntry castBackendEntry(
              BackendEntry entry) {
        assert entry instanceof CassandraBackendEntry;
        if (!(entry instanceof CassandraBackendEntry)) {
            throw new BackendException(
                      "Cassandra store only supports CassandraBackendEntry");
        }
        return (CassandraBackendEntry) entry;
    }

    /***************************** Store defines *****************************/

    public static class CassandraSchemaStore extends CassandraStore {

        public CassandraSchemaStore(BackendStoreProvider provider,
                                    String keyspace, String name) {
            super(provider, keyspace, name);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new CassandraTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new CassandraTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new CassandraTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new CassandraTables.IndexLabel());
        }
    }

    public static class CassandraGraphStore extends CassandraStore {

        public CassandraGraphStore(BackendStoreProvider provider,
                                   String keyspace, String name) {
            super(provider, keyspace, name);

            registerTableManager(HugeType.VERTEX,
                                 new CassandraTables.Vertex());
            registerTableManager(HugeType.EDGE,
                                 new CassandraTables.Edge());

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new CassandraTables.SecondaryIndex());
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new CassandraTables.SearchIndex());
        }
    }
}
