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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.rocksdb.RocksDBException;
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
    private Map<HugeType, String> tableDiskMapping;

    // DataPath:RocksDB mapping
    private static final Map<String, RocksDBSessions> dbs =
                                                      new ConcurrentHashMap<>();

    public RocksDBStore(final BackendStoreProvider provider,
                        final String database, final String name) {
        this.tables = new HashMap<>();

        this.provider = provider;
        this.database = database;
        this.name = name;
        this.conf = null;
        this.sessions = null;
        this.tableDiskMapping = new HashMap<>();;
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

        // Open base disk
        String data = this.wrapPath(this.conf.get(RocksDBOptions.DATA_PATH));
        String wal = this.wrapPath(this.conf.get(RocksDBOptions.WAL_PATH));
        this.sessions = this.open(data, wal, this.tableNames());

        // Open tables with optimized disk
        List<String> disks = this.conf.get(RocksDBOptions.DATA_DISKS);
        if (!disks.isEmpty()) {
            this.parseTableDiskMapping(disks);
            for (Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
                String table = this.table(e.getKey()).table();
                String disk = e.getValue();
                this.open(disk, disk, Arrays.asList(table));
            }
        }
    }

    protected RocksDBSessions open(String data, String wal, List<String> tbs) {
        LOG.info("Opening RocksDB with data path: {}", data);

        RocksDBSessions sessions = null;
        try {
            sessions = newSessions(this.conf, data, wal, tbs);
        } catch (RocksDBException e) {
            if (dbs.containsKey(data)) {
                if (e.getMessage().contains("No locks available")) {
                    // Open twice, but we should support keyspace
                    sessions = dbs.get(data);
                } else if (e.getMessage().contains("Column family not found")) {
                    // Open a keyspace after other keyspace closed
                    try {
                        // Will open old CFs(of other keyspace)
                        final List<String> none = ImmutableList.of();
                        sessions = newSessions(this.conf, data, wal, none);
                    } catch (RocksDBException e1) {
                        // Let it throw later
                        e = e1;
                    }
                }
            } else if (e.getMessage().contains("Column family not found")) {
                // Before init the first keyspace
                LOG.info("Failed to open RocksDB '{}' with database '{}', " +
                         "try to init CF later", data, this.database);
                try {
                    // Only open default CF, won't open old CFs
                    sessions = newSessions(this.conf, data, wal, null);
                } catch (RocksDBException e1) {
                    LOG.error("Failed to open RocksDB with default CF", e1);
                }
            }

            if (sessions == null) {
                // Error after trying other ways
                LOG.error("Failed to open RocksDB '{}'", data, e);
                throw new BackendException("Failed to open RocksDB '%s'",
                                           e, data);
            }
        }

        if (sessions != null) {
            dbs.put(data, sessions);
            sessions.session();
            LOG.debug("Store opened: {}", data);
        }

        return sessions;
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

        for (HugeType type : mutation.types()) {
            Session session = this.session(type);
            for (Iterator<BackendAction> it = mutation.mutation(type);
                 it.hasNext();) {
                this.mutate(session, it.next());
            }
        }
    }

    private void mutate(Session session, BackendAction item) {
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
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        HugeType tableType = RocksDBTable.tableType(query);
        RocksDBTable table = this.table(tableType);
        return table.query(this.session(tableType), query);
    }

    @Override
    public void init() {
        this.checkOpened();

        for (String table : this.tableNames()) {
            this.createTable(this.sessions, table);
        }

        // Create table with optimized disk
        for (Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
            String table = this.table(e.getKey()).table();
            RocksDBSessions db = db(e.getValue());
            this.createTable(db, table);
        }

        LOG.info("Store initialized: {}", this.name);
    }

    private void createTable(RocksDBSessions db, String table) {
        try {
            db.createTable(table);
        } catch (RocksDBException e) {
            throw new BackendException("Failed to create '%s' for '%s'",
                                       e, table, this.name);
        }
    }

    @Override
    public void clear() {
        this.checkOpened();

        for (String table : this.tableNames()) {
            this.dropTable(this.sessions, table);
        }

        // Drop table with optimized disk
        for (Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
            String table = this.table(e.getKey()).table();
            RocksDBSessions db = db(e.getValue());
            this.dropTable(db, table);
        }

        LOG.info("Store cleared: {}", this.name);
    }

    private void dropTable(RocksDBSessions db, String table) {
        try {
            this.sessions.dropTable(table);
        } catch (BackendException e) {
            if (e.getMessage().contains("is not opened")) {
                return;
            }
            throw e;
        } catch (RocksDBException e) {
            throw new BackendException("Failed to drop '%s' for '%s'",
                                       e, table, this.name);
        }
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        this.checkOpened();
        // Unable to guarantee atomicity when committing multi sessions
        for (Session session : this.session()) {
            Object count = session.commit();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Store {} committed {} items", this.name, count);
            }
        }
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();

        for (Session session : this.session()) {
            session.clear();
        }
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        RocksDBTable table = this.table(type);
        return table.metadata(this.session(type), meta, args);
    }

    private Session session(HugeType tableType) {
        this.checkOpened();

        // Optimized disk
        String disk = this.tableDiskMapping.get(tableType);
        if (disk != null) {
            return db(disk).session();
        }

        return this.sessions.session();
    }

    private List<Session> session() {
        this.checkOpened();

        if (this.tableDiskMapping.isEmpty()) {
            return Arrays.asList(this.sessions.session());
        }

        // Collect session of each table with optimized disk
        List<Session> list = new ArrayList<>(this.tableDiskMapping.size() + 1);
        list.add(this.sessions.session());
        for (String disk : this.tableDiskMapping.values()) {
            list.add(db(disk).session());
        }
        return list;
    }

    private void checkOpened() {
        E.checkState(this.sessions != null && !this.sessions.closed(),
                     "RocksDB store has not been opened");
    }

    private void parseTableDiskMapping(List<String> disks) {
        this.tableDiskMapping.clear();
        for (String disk : disks) {
            // The format of `disk` like: `graph/vertex: /path/to/disk1`
            String[] pair = disk.split(":", 2);
            E.checkState(pair.length == 2,
                         "Invalid disk format: '%s', expect `NAME:PATH`", disk);
            String name = pair[0].trim();
            String path = pair[1].trim();
            pair = name.split("/", 2);
            E.checkState(pair.length == 2,
                         "Invalid disk key format: '%s', expect `STORE/TABLE`",
                         name);
            String store = pair[0].trim();
            HugeType table = HugeType.valueOf(pair[1].trim().toUpperCase());
            if (this.name.equals(store)) {
                this.tableDiskMapping.put(table, this.wrapPath(path));
            }
        }
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

    private static RocksDBSessions db(String disk) {
        RocksDBSessions db = dbs.get(disk);
        E.checkState(db != null && !db.closed(),
                     "RocksDB store has not been opened: %s", disk);
        return db;
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

            registerTableManager(HugeType.EDGE_OUT,
                                 RocksDBTables.Edge.out(database));
            registerTableManager(HugeType.EDGE_IN,
                                 RocksDBTables.Edge.in(database));

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
