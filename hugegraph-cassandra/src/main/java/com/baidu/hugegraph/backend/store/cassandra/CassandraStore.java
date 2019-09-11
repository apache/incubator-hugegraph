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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.AbstractBackendStore;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
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

public abstract class CassandraStore
                extends AbstractBackendStore<CassandraSessionPool.Session> {

    private static final Logger LOG = Log.logger(CassandraStore.class);

    private static final BackendFeatures FEATURES = new CassandraFeatures();

    private final String store;
    private final String keyspace;

    private final BackendStoreProvider provider;
    // TODO: move to parent class
    private final Map<HugeType, CassandraTable> tables;

    private CassandraSessionPool sessions;
    private HugeConfig conf;

    public CassandraStore(final BackendStoreProvider provider,
                          final String keyspace, final String store) {
        E.checkNotNull(keyspace, "keyspace");
        E.checkNotNull(store, "store");

        this.provider = provider;

        this.keyspace = keyspace;
        this.store = store;
        this.tables = new ConcurrentHashMap<>();

        this.sessions = null;
        this.conf = null;

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            CassandraMetrics metrics = new CassandraMetrics(cluster(), this.conf);
            return metrics.getMetrics();
        });
    }

    protected void registerTableManager(HugeType type, CassandraTable table) {
        this.tables.put(type, table);
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.keyspace;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.store);
        E.checkNotNull(config, "config");

        if (this.sessions == null) {
            this.sessions = new CassandraSessionPool(config, this.keyspace,
                                                     this.store);
        }

        if (this.sessions.opened()) {
            // TODO: maybe we should throw an exception here instead of ignore
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }
        this.conf = config;

        // Init cluster
        this.sessions.open();

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

        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);
        this.sessions.close();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkSessionConnected();
        CassandraSessionPool.Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(CassandraSessionPool.Session session,
                        BackendAction item) {
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
                throw new AssertionError(String.format(
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkSessionConnected();

        CassandraTable table = this.table(CassandraTable.tableType(query));
        return table.query(this.sessions.session(), query);
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public void init() {
        this.checkClusterConnected();

        if (this.sessions.session().opened()) {
            // Session has ever been opened.
            LOG.warn("Session has ever been opened(exist keyspace '{}' before)",
                     this.keyspace);
        } else {
            // Create keyspace if needed
            if (!this.existsKeyspace()) {
                this.initKeyspace();
            }
            // Open session explicitly to get the exception when it fails
            this.sessions.session().open();
        }

        // Create tables
        this.checkSessionConnected();
        this.initTables();

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear() {
        this.checkClusterConnected();

        if (this.existsKeyspace()) {
            this.checkSessionConnected();
            this.clearTables();
            this.clearKeyspace();
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
    public void beginTx() {
        this.checkSessionConnected();

        CassandraSessionPool.Session session = this.sessions.session();
        if (session.txState() != TxState.CLEAN) {
            LOG.warn("Store {} expect state CLEAN than {} when begin()",
                     this.store, session.txState());
        }
        session.txState(TxState.BEGIN);
    }

    @Override
    public void commitTx() {
        this.checkSessionConnected();

        CassandraSessionPool.Session session = this.sessions.session();
        if (session.txState() != TxState.BEGIN) {
            LOG.warn("Store {} expect state BEGIN than {} when commit()",
                     this.store, session.txState());
        }

        if (!session.hasChanges()) {
            session.txState(TxState.CLEAN);
            LOG.debug("Store {} has nothing to commit", this.store);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} commit {} statements: {}", this.store,
                      session.statements().size(), session.statements());
        }

        // TODO how to implement tx perfectly?

        // Do update
        session.txState(TxState.COMMITTING);
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
            session.txState() != TxState.CLEAN) {
            LOG.warn("Store {} expect state COMMITT_FAIL/COMMITTING/CLEAN " +
                     "than {} when rollback()", this.store, session.txState());
        }

        session.txState(TxState.ROLLBACKING);
        try {
            session.rollback();
        } finally {
            // Assume batch commit would auto rollback
            session.txState(TxState.CLEAN);
        }
    }

    protected Cluster cluster() {
        return this.sessions.cluster();
    }

    protected void initKeyspace() {
        Statement stmt = SchemaBuilder.createKeyspace(this.keyspace)
                                      .ifNotExists().with()
                                      .replication(parseReplica(this.conf));
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
    }

    private static Map<String, Object> parseReplica(HugeConfig conf) {
        Map<String, Object> replication = new HashMap<>();
        // Replication strategy: SimpleStrategy or NetworkTopologyStrategy
        String strategy = conf.get(CassandraOptions.CASSANDRA_STRATEGY);
        replication.put("class", strategy);

        switch (strategy) {
            case "SimpleStrategy":
                List<String> replicas =
                             conf.get(CassandraOptions.CASSANDRA_REPLICATION);
                E.checkArgument(replicas.size() == 1,
                                "Individual factor value should be provided " +
                                "with SimpleStrategy for Cassandra");
                int factor = convertFactor(replicas.get(0));
                replication.put("replication_factor", factor);
                break;
            case "NetworkTopologyStrategy":
                // The replicas format is like 'dc1:2,dc2:1'
                Map<String, String> replicaMap =
                            conf.getMap(CassandraOptions.CASSANDRA_REPLICATION);
                for (Map.Entry<String, String> e : replicaMap.entrySet()) {
                    E.checkArgument(!e.getKey().isEmpty(),
                                    "The datacenter can't be empty");
                    replication.put(e.getKey(), convertFactor(e.getValue()));
                }
                break;
            default:
                throw new AssertionError(String.format(
                          "Illegal replication strategy '%s', valid strategy " +
                          "is 'SimpleStrategy' or 'NetworkTopologyStrategy'",
                          strategy));
        }
        return replication;
    }

    private static int convertFactor(String factor) {
        try {
            return Integer.valueOf(factor);
        } catch (NumberFormatException e) {
            throw new HugeException(
                      "Expect int factor value for SimpleStrategy, " +
                      "but got '%s'", factor);
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
        for (CassandraTable table : this.tables()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables()) {
            table.clear(session);
        }
    }

    protected void truncateTables() {
        CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables()) {
            table.truncate(session);
        }
    }

    protected Collection<CassandraTable> tables() {
        return this.tables.values();
    }

    @Override
    protected final CassandraTable table(HugeType type) {
        assert type != null;
        CassandraTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected CassandraSessionPool.Session session(HugeType type) {
        this.checkSessionConnected();
        return this.sessions.session();
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
        assert entry instanceof CassandraBackendEntry : entry.getClass();
        if (!(entry instanceof CassandraBackendEntry)) {
            throw new BackendException(
                      "Cassandra store only supports CassandraBackendEntry");
        }
        return (CassandraBackendEntry) entry;
    }

    /***************************** Store defines *****************************/

    public static class CassandraSchemaStore extends CassandraStore {

        private final CassandraTables.Counters counters;

        public CassandraSchemaStore(BackendStoreProvider provider,
                                    String keyspace, String store) {
            super(provider, keyspace, store);

            this.counters = new CassandraTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new CassandraTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new CassandraTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new CassandraTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new CassandraTables.IndexLabel());
        }

        @Override
        protected Collection<CassandraTable> tables() {
            List<CassandraTable> tables = new ArrayList<>(super.tables());
            tables.add(this.counters);
            return tables;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.checkSessionConnected();
            CassandraSessionPool.Session session = super.sessions.session();
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            this.checkSessionConnected();
            CassandraSessionPool.Session session = super.sessions.session();
            return this.counters.getCounter(session, type);
        }
    }

    public static class CassandraGraphStore extends CassandraStore {

        public CassandraGraphStore(BackendStoreProvider provider,
                                   String keyspace, String store) {
            super(provider, keyspace, store);

            registerTableManager(HugeType.VERTEX,
                                 new CassandraTables.Vertex(store));

            registerTableManager(HugeType.EDGE_OUT,
                                 CassandraTables.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                                 CassandraTables.Edge.in(store));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new CassandraTables.SecondaryIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new CassandraTables.RangeIntIndex(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new CassandraTables.RangeFloatIndex(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new CassandraTables.RangeLongIndex(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new CassandraTables.RangeDoubleIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new CassandraTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new CassandraTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new CassandraTables.UniqueIndex(store));
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "CassandraGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "CassandraGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "CassandraGraphStore.getCounter()");
        }
    }
}
