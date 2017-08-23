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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
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

        this.initTableManagers();

        LOG.debug("Store loaded: {}", name);
    }

    protected abstract void initTableManagers();

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
        E.checkNotNull(config, "config");

        if (this.sessions.opened()) {
            // TODO: maybe we should throw an exception here instead of ignore
            LOG.debug("Store {} has been opened before", this.name);
            this.sessions.useSession();
            return;
        }
        this.conf = config;

        String hosts = config.get(CassandraOptions.CASSANDRA_HOST);
        int port = config.get(CassandraOptions.CASSANDRA_PORT);

        // Init cluster
        this.sessions.open(hosts, port);

        // Init a session for current thread
        try {
            LOG.debug("Store connect with keyspace: {}", this.keyspace);
            try {
                this.sessions.session();
            } catch (InvalidQueryException e) {
                // TODO: the error message may be changed in different versions
                if (!e.getMessage().contains(String.format(
                    "Keyspace '%s' does not exist", this.keyspace))) {
                    throw e;
                }
                LOG.info("Failed to connect keyspace: {}, " +
                         "try init keyspace later", this.keyspace);
                this.sessions.closeSession();
            }
        } catch (Throwable e) {
            try {
                this.sessions.close();
            } catch (Throwable e2) {
                LOG.warn("Failed to close cluster", e2);
            }
            throw e;
        }

        LOG.debug("Store opened: {}", this.name);
    }

    @Override
    public void close() {
        this.sessions.close();
        LOG.debug("Store closed: {}", this.name);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        LOG.debug("Store {} mutation: {}", this.name, mutation);

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
    public Iterable<BackendEntry> query(Query query) {
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
    public void init() {
        this.checkClusterConnected();

        this.initKeyspace();
        this.initTables();

        LOG.info("Store initialized: {}", this.name);
    }

    @Override
    public void clear() {
        this.checkClusterConnected();

        if (this.existsKeyspace()) {
            this.clearTables();
            this.clearKeyspace();
        }

        LOG.info("Store cleared: {}", this.name);
    }

    @Override
    public void beginTx() {
        // TODO how to implement?
    }

    @Override
    public void commitTx() {
        this.checkSessionConnected();

        // Do update
        CassandraSessionPool.Session session = this.sessions.session();
        if (!session.hasChanged()) {
            LOG.debug("Store {} has nothing to commit", this.name);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} commit statements: {}",
                      this.name, session.statements());
        }

        try {
            session.commit();
        } catch (DriverException e) {
            LOG.error("Failed to commit statements due to:", e);
            assert session.statements().size() > 0;
            throw new BackendException(
                      "Failed to commit %s statements: '%s'...",
                      session.statements().size(),
                      session.statements().iterator().next());
        } finally {
            session.clear();
        }

        // TODO how to implement tx?
    }

    @Override
    public void rollbackTx() {
        // TODO how to implement?
        throw new UnsupportedOperationException(
                  "Unsupported rollback operation by Cassandra");
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
        int replication = this.conf.get(CassandraOptions.CASSANDRA_REPLICATION);

        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                     "WITH replication={'class':'%s','replication_factor':%d}",
                     this.keyspace, strategy, replication);

        // Create keyspace with non-keyspace-session
        LOG.debug("Create keyspace: {}", cql);
        Session session = this.cluster().connect();
        try {
            session.execute(cql);
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
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
            throw new BackendException("Unsupported type: %s", type.name());
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
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeType.VERTEX_LABEL,
                             new CassandraTables.VertexLabel());
            super.tables.put(HugeType.EDGE_LABEL,
                             new CassandraTables.EdgeLabel());
            super.tables.put(HugeType.PROPERTY_KEY,
                             new CassandraTables.PropertyKey());
            super.tables.put(HugeType.INDEX_LABEL,
                             new CassandraTables.IndexLabel());
        }
    }

    public static class CassandraGraphStore extends CassandraStore {

        public CassandraGraphStore(BackendStoreProvider provider,
                                   String keyspace, String name) {
            super(provider, keyspace, name);
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeType.VERTEX, new CassandraTables.Vertex());
            super.tables.put(HugeType.EDGE, new CassandraTables.Edge());
        }
    }

    public static class CassandraIndexStore extends CassandraStore {

        public CassandraIndexStore(BackendStoreProvider provider,
                                   String keyspace, String name) {
            super(provider, keyspace, name);
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeType.SECONDARY_INDEX,
                             new CassandraTables.SecondaryIndex());
            super.tables.put(HugeType.SEARCH_INDEX,
                             new CassandraTables.SearchIndex());
        }
    }
}
