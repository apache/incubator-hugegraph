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

package com.baidu.hugegraph.backend.store.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
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
import com.baidu.hugegraph.backend.store.hbase.HbaseSessions.Session;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class HbaseStore extends AbstractBackendStore<Session> {

    private static final Logger LOG = Log.logger(HbaseStore.class);

    private static final BackendFeatures FEATURES = new HbaseFeatures();

    private final String store;
    private final String namespace;

    private final BackendStoreProvider provider;
    private final Map<HugeType, HbaseTable> tables;

    private HbaseSessions sessions;

    public HbaseStore(final BackendStoreProvider provider,
                      final String namespace, final String store) {
        this.tables = new HashMap<>();

        this.provider = provider;
        this.namespace = namespace;
        this.store = store;
        this.sessions = null;

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            HbaseMetrics metrics = new HbaseMetrics(this.sessions);
            return metrics.metrics();
        });

        this.registerMetaHandler("compact", (session, meta, args) -> {
            HbaseMetrics metrics = new HbaseMetrics(this.sessions);
            return metrics.compact(this.tableNames());
        });
    }

    protected void registerTableManager(HugeType type, HbaseTable table) {
        this.tables.put(type, table);
    }

    @Override
    protected final HbaseTable table(HugeType type) {
        assert type != null;
        HbaseTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected Session session(HugeType type) {
        this.checkOpened();
        return this.sessions.session();
    }

    protected List<String> tableNames() {
        return this.tables.values().stream().map(t -> t.table())
                                            .collect(Collectors.toList());
    }

    public String namespace() {
        return this.namespace;
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.namespace;
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
        E.checkNotNull(config, "config");

        if (this.sessions == null) {
            this.sessions = new HbaseSessions(config, this.namespace, this.store);
        }

        assert this.sessions != null;
        if (!this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        try {
            // NOTE: won't throw error even if connection refused
            this.sessions.open();
        } catch (Exception e) {
            if (!e.getMessage().contains("Column family not found")) {
                LOG.error("Failed to open HBase '{}'", this.store, e);
                throw new ConnectionException("Failed to connect to HBase", e);
            }
            if (this.isSchemaStore()) {
                LOG.info("Failed to open HBase '{}' with database '{}', " +
                         "try to init CF later", this.store, this.namespace);
            }
        }

        this.sessions.session();
        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        this.checkOpened();
        this.sessions.close();

        LOG.debug("Store closed: {}", this.store);
    }

    @Override
    public boolean opened() {
        this.checkConnectionOpened();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkOpened();
        Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(Session session, BackendAction item) {
        BackendEntry entry = item.entry();
        HbaseTable table = this.table(entry.type());

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
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();

        Session session = this.sessions.session();
        HbaseTable table = this.table(HbaseTable.tableType(query));
        return table.query(session, query);
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        Session session = this.sessions.session();
        HbaseTable table = this.table(HbaseTable.tableType(query));
        return table.queryNumber(session, query);
    }

    @Override
    public void init() {
        this.checkConnectionOpened();

        // Create namespace
        try {
            this.sessions.createNamespace();
        } catch (NamespaceExistException ignored) {
            // Ignore due to both schema & graph store would create namespace
        } catch (IOException e) {
            throw new BackendException(
                      "Failed to create namespace '%s' for '%s' store",
                      e, this.namespace, this.store);
        }

        // Create tables
        for (String table : this.tableNames()) {
            try {
                this.sessions.createTable(table, HbaseTable.cfs());
            } catch (TableExistsException ignored) {
                continue;
            } catch (IOException e) {
                throw new BackendException(
                          "Failed to create table '%s' for '%s' store",
                          e, table, this.store);
            }
        }

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        this.checkConnectionOpened();

        // Return if not exists namespace
        try {
            if (!this.sessions.existsNamespace()) {
                return;
            }
        } catch (IOException e) {
            throw new BackendException(
                      "Exception when checking for the existence of '%s'",
                      e, this.namespace);
        }

        if (!clearSpace) {
            // Drop tables
            for (String table : this.tableNames()) {
                try {
                    this.sessions.dropTable(table);
                } catch (TableNotFoundException e) {
                    LOG.warn("The table '{}' of '{}' store does not exist " +
                             "when trying to drop", table, this.store);
                } catch (IOException e) {
                    throw new BackendException(
                              "Failed to drop table '%s' of '%s' store",
                              e, table, this.store);
                }
            }
        } else {
            // Drop namespace
            try {
                this.sessions.dropNamespace();
            } catch (IOException e) {
                String notEmpty = "Only empty namespaces can be removed";
                if (e.getCause().getMessage().contains(notEmpty)) {
                    LOG.debug("Can't drop namespace '{}': {}",
                              this.namespace, e);
                } else {
                    throw new BackendException(
                              "Failed to drop namespace '%s' of '%s' store",
                              e, this.namespace, this.store);
                }
            }
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public boolean initialized() {
        this.checkConnectionOpened();

        try {
            if (!this.sessions.existsNamespace()) {
                return false;
            }
            for (String table : this.tableNames()) {
                if (!this.sessions.existsTable(table)) {
                    return false;
                }
            }
        } catch (IOException e) {
            throw new BackendException("Failed to obtain table info", e);
        }
        return true;
    }

    @Override
    public void truncate() {
        this.checkOpened();

        // Total time may cost 3 * TRUNCATE_TIMEOUT, due to there are 3 stores
        long timeout = this.sessions.config().get(HbaseOptions.TRUNCATE_TIMEOUT);
        long start = System.currentTimeMillis();

        BiFunction<String, Future<Void>, Void> wait = (table, future) -> {
            long elapsed = System.currentTimeMillis() - start;
            long remainingTime = timeout - elapsed / 1000L;
            try {
                return future.get(remainingTime, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new BackendException(
                          "Error when truncating table '%s' of '%s' store: %s",
                          table, this.store, e.toString());
            }
        };

        // Truncate tables
        List<String> tables = this.tableNames();
        Map<String, Future<Void>> futures = new HashMap<>(tables.size());

        try {
            // Disable tables async
            for (String table : tables) {
                futures.put(table, this.sessions.disableTableAsync(table));
            }
            for (Map.Entry<String, Future<Void>> entry : futures.entrySet()) {
                wait.apply(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            this.enableTables();
            throw new BackendException(
                      "Failed to disable table for '%s' store", e, this.store);
        }

        try {
            // Truncate tables async
            for (String table : tables) {
                futures.put(table, this.sessions.truncateTableAsync(table));
            }
            for (Map.Entry<String, Future<Void>> entry : futures.entrySet()) {
                wait.apply(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            this.enableTables();
            throw new BackendException(
                      "Failed to truncate table for '%s' store", e, this.store);
        }

        LOG.debug("Store truncated: {}", this.store);
    }

    private void enableTables() {
        for (String table : this.tableNames()) {
            try {
                this.sessions.enableTable(table);
            } catch (Exception e) {
                LOG.warn("Failed to enable table '{}' of '{}' store",
                         table, this.store, e);
            }
        }
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        this.checkOpened();
        Session session = this.sessions.session();

        session.commit();
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();
        Session session = this.sessions.session();

        session.rollback();
    }

    private final void checkConnectionOpened() {
        E.checkState(this.sessions != null && this.sessions.opened(),
                     "HBase store has not been initialized");
    }

    /***************************** Store defines *****************************/

    public static class HbaseSchemaStore extends HbaseStore {

        private final HbaseTables.Counters counters;

        public HbaseSchemaStore(BackendStoreProvider provider,
                                String namespace, String store) {
            super(provider, namespace, store);

            this.counters = new HbaseTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new HbaseTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new HbaseTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new HbaseTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new HbaseTables.IndexLabel());

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HbaseTables.SecondaryIndex(store));
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.counters.table());
            return tableNames;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            super.checkOpened();
            this.counters.increaseCounter(super.sessions.session(),
                                          type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            super.checkOpened();
            return this.counters.getCounter(super.sessions.session(), type);
        }

        @Override
        public String storedVersion() {
            throw new UnsupportedOperationException(
                      "HbaseSchemaStore.storedVersion()");
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class HbaseGraphStore extends HbaseStore {

        public HbaseGraphStore(BackendStoreProvider provider,
                               String namespace, String store) {
            super(provider, namespace, store);

            registerTableManager(HugeType.VERTEX,
                                 new HbaseTables.Vertex(store));

            registerTableManager(HugeType.EDGE_OUT,
                                 HbaseTables.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                                 HbaseTables.Edge.in(store));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HbaseTables.SecondaryIndex(store));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX,
                                 new HbaseTables.VertexLabelIndex(store));
            registerTableManager(HugeType.EDGE_LABEL_INDEX,
                                 new HbaseTables.EdgeLabelIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 HbaseTables.RangeIndex.rangeInt(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 HbaseTables.RangeIndex.rangeFloat(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 HbaseTables.RangeIndex.rangeLong(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 HbaseTables.RangeIndex.rangeDouble(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new HbaseTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new HbaseTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new HbaseTables.UniqueIndex(store));
        }

        @Override
        public String storedVersion() {
            throw new UnsupportedOperationException(
                      "HbaseGraphStore.storedVersion()");
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "HbaseGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "HbaseGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "HbaseGraphStore.getCounter()");
        }

        public Session getSession() {
            return super.sessions.session();
        }
    }

    public static class HbaseSystemStore extends HbaseGraphStore {

        private final HbaseTables.Meta meta;

        public HbaseSystemStore(BackendStoreProvider provider,
                                String namespace, String store) {
            super(provider, namespace, store);

            this.meta = new HbaseTables.Meta();
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.meta.table());
            return tableNames;
        }

        @Override
        public void init() {
            super.init();
            super.checkOpened();
            Session session = super.getSession();
            String driverVersion = this.provider().driverVersion();
            this.meta.writeVersion(session, driverVersion);
            LOG.info("Write down the backend version: {}", driverVersion);
        }

        @Override
        public String storedVersion() {
            super.checkOpened();
            Session session = super.getSession();
            return this.meta.readVersion(session);
        }
    }
}
