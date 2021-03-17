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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.AbstractBackendStore;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Consumers;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.GZipUtil;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public abstract class RocksDBStore extends AbstractBackendStore<Session> {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private static final BackendFeatures FEATURES = new RocksDBFeatures();

    private final String store;
    private final String database;

    private final BackendStoreProvider provider;
    private final Map<HugeType, RocksDBTable> tables;

    private RocksDBSessions sessions;
    private final Map<HugeType, String> tableDiskMapping;
    // DataPath:RocksDB mapping
    private final ConcurrentMap<String, RocksDBSessions> dbs;
    private final ReadWriteLock storeLock;

    private static final String DB_OPEN = "db-open-%s";
    private static final long OPEN_TIMEOUT = 600L;
    /*
     * This is threads number used to concurrently opening RocksDB dbs,
     * 8 is supposed enough due to configurable data disks and
     * disk number of one machine
     */
    private static final int OPEN_POOL_THREADS = 8;

    public RocksDBStore(final BackendStoreProvider provider,
                        final String database, final String store) {
        this.tables = new HashMap<>();

        this.provider = provider;
        this.database = database;
        this.store = store;
        this.sessions = null;
        this.tableDiskMapping = new HashMap<>();
        this.dbs = new ConcurrentHashMap<>();
        this.storeLock = new ReentrantReadWriteLock();

        this.registerMetaHandlers();
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            List<RocksDBSessions> dbs = new ArrayList<>();
            dbs.add(this.sessions);
            dbs.addAll(this.tableDBMapping().values());

            RocksDBMetrics metrics = new RocksDBMetrics(dbs, session);
            return metrics.getMetrics();
        });
    }

    protected void registerTableManager(HugeType type, RocksDBTable table) {
        this.tables.put(type, table);
    }

    @Override
    protected final RocksDBTable table(HugeType type) {
        assert type != null;
        RocksDBTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    protected List<String> tableNames() {
        return this.tables.values().stream().map(BackendTable::table)
                                            .collect(Collectors.toList());
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
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.store);

        E.checkNotNull(config, "config");

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.useSessions();
            return;
        }

        List<Future<?>> futures = new ArrayList<>();
        ExecutorService openPool = ExecutorUtil.newFixedThreadPool(
                                   OPEN_POOL_THREADS, DB_OPEN);
        // Open base disk
        futures.add(openPool.submit(() -> {
            this.sessions = this.open(config, this.tableNames());
        }));

        // Open tables with optimized disk
        Map<String, String> disks = config.getMap(RocksDBOptions.DATA_DISKS);
        Set<String> openedDisks = new HashSet<>();
        if (!disks.isEmpty()) {
            String dataPath = config.get(RocksDBOptions.DATA_PATH);
            this.parseTableDiskMapping(disks, dataPath);
            for (Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
                String table = this.table(e.getKey()).table();
                String disk = e.getValue();
                if (openedDisks.contains(disk)) {
                    continue;
                }
                openedDisks.add(disk);
                futures.add(openPool.submit(() -> {
                    this.open(config, disk, disk, Arrays.asList(table));
                }));
            }
        }
        this.waitOpenFinish(futures, openPool);
    }

    private void waitOpenFinish(List<Future<?>> futures,
                                ExecutorService openPool) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Throwable e) {
                throw new BackendException("Failed to open RocksDB store", e);
            }
        }
        if (openPool.isShutdown()) {
            return;
        }

        /*
         * Transfer the session holder from db-open thread to main thread,
         * otherwise once the db-open thread pool is closed, we can no longer
         * close the session created by it, which will cause the rocksdb
         * instance fail to close
         */
        this.useSessions();
        try {
            Consumers.executeOncePerThread(openPool, OPEN_POOL_THREADS,
                                           this::closeSessions);
        } catch (InterruptedException e) {
            throw new BackendException("Failed to close session opened by " +
                                       "open-pool");
        }

        boolean terminated = false;
        openPool.shutdown();
        try {
            terminated = openPool.awaitTermination(OPEN_TIMEOUT,
                                                   TimeUnit.SECONDS);
        } catch (Throwable e) {
            throw new BackendException(
                      "Failed to wait db-open thread pool shutdown", e);
        }
        if (!terminated) {
            LOG.warn("Timeout when waiting db-open thread pool shutdown");
        }
        openPool.shutdownNow();
    }

    protected RocksDBSessions open(HugeConfig config, List<String> tableNames) {
        String dataPath = this.wrapPath(config.get(RocksDBOptions.DATA_PATH));
        String walPath = this.wrapPath(config.get(RocksDBOptions.WAL_PATH));
        return this.open(config, dataPath, walPath, tableNames);
    }

    protected RocksDBSessions open(HugeConfig config, String dataPath,
                                   String walPath, List<String> tableNames) {
        LOG.info("Opening RocksDB with data path: {}", dataPath);

        RocksDBSessions sessions = null;
        try {
            sessions = this.openSessionPool(config, dataPath,
                                            walPath, tableNames);
        } catch (RocksDBException e) {
            RocksDBSessions origin = this.dbs.get(dataPath);
            if (origin != null) {
                if (e.getMessage().contains("No locks available")) {
                    /*
                     * Open twice, copy a RocksDBSessions reference, since from
                     * v0.11.2 release we don't support multi graphs share
                     * rocksdb instance (before v0.11.2 graphs with different
                     * CF-prefix share one rocksdb instance and data path),
                     * so each graph has its independent data paths, but multi
                     * CFs may share same optimized disk(or optimized disk path).
                     */
                    sessions = origin.copy(config, this.database, this.store);
                }
            }

            if (e.getMessage().contains("Column family not found")) {
                if (this.isSchemaStore()) {
                    LOG.info("Failed to open RocksDB '{}' with database '{}'," +
                             " try to init CF later", dataPath, this.database);
                }
                List<String> none;
                boolean existsOtherKeyspace = existsOtherKeyspace(dataPath);
                if (existsOtherKeyspace) {
                    // Open a keyspace after other keyspace closed
                    // Set to empty list to open old CFs(of other keyspace)
                    none = ImmutableList.of();
                } else {
                    // Before init the first keyspace
                    none = null;
                }
                try {
                    sessions = this.openSessionPool(config, dataPath,
                                                    walPath, none);
                } catch (RocksDBException e1) {
                    e = e1;
                }
                if (sessions == null && !existsOtherKeyspace) {
                    LOG.error("Failed to open RocksDB with default CF, " +
                              "is there data for other programs: {}", dataPath);
                }
            }

            if (sessions == null) {
                // Error after trying other ways
                LOG.error("Failed to open RocksDB '{}'", dataPath, e);
                throw new ConnectionException("Failed to open RocksDB '%s'",
                                              e, dataPath);
            }
        }

        if (sessions != null) {
            // May override the original session pool
            this.dbs.put(dataPath, sessions);
            sessions.session();
            LOG.debug("Store opened: {}", dataPath);
        }

        return sessions;
    }

    protected RocksDBSessions openSessionPool(HugeConfig config,
                                              String dataPath, String walPath,
                                              List<String> tableNames)
                                              throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBStdSessions(config, this.database, this.store,
                                          dataPath, walPath);
        } else {
            return new RocksDBStdSessions(config, this.database, this.store,
                                          dataPath, walPath, tableNames);
        }
    }

    protected String wrapPath(String path) {
        // Ensure the `path` exists
        try {
            FileUtils.forceMkdir(FileUtils.getFile(path));
        } catch (IOException e) {
            throw new BackendException(e.getMessage(), e);
        }
        // Join with store type
        return Paths.get(path, this.store).toString();
    }

    protected Map<String, RocksDBSessions> tableDBMapping() {
        Map<String, RocksDBSessions> tableDBMap = InsertionOrderUtil.newMap();
        for (Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
            String table = this.table(e.getKey()).table();
            RocksDBSessions db = this.db(e.getValue());
            tableDBMap.put(table, db);
        }
        return tableDBMap;
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);

        this.checkOpened();
        this.closeSessions();
    }

    @Override
    public boolean opened() {
        this.checkDbOpened();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Store {} mutation: {}", this.store, mutation);
            }

            for (HugeType type : mutation.types()) {
                Session session = this.session(type);
                for (Iterator<BackendAction> it = mutation.mutation(type);
                     it.hasNext();) {
                    this.mutate(session, it.next());
                }
            }
        } finally {
            readLock.unlock();
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
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            HugeType tableType = RocksDBTable.tableType(query);
            RocksDBTable table = this.table(tableType);
            return table.query(this.session(tableType), query);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Number queryNumber(Query query) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            HugeType tableType = RocksDBTable.tableType(query);
            RocksDBTable table = this.table(tableType);
            return table.queryNumber(this.session(tableType), query);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized void init() {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkDbOpened();

            // Create tables with main disk
            this.createTable(this.sessions,
                             this.tableNames().toArray(new String[0]));

            // Create table with optimized disk
            Map<String, RocksDBSessions> tableDBMap = this.tableDBMapping();
            for (Map.Entry<String, RocksDBSessions> e : tableDBMap.entrySet()) {
                this.createTable(e.getValue(), e.getKey());
            }

            LOG.debug("Store initialized: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    private void createTable(RocksDBSessions db, String... tables) {
        try {
            db.createTable(tables);
        } catch (RocksDBException e) {
            throw new BackendException("Failed to create tables %s for '%s'",
                                       e, Arrays.asList(tables), this.store);
        }
    }

    @Override
    public synchronized void clear(boolean clearSpace) {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkDbOpened();

            // Drop tables with main disk
            this.dropTable(this.sessions,
                           this.tableNames().toArray(new String[0]));

            // Drop tables with optimized disk
            Map<String, RocksDBSessions> tableDBMap = this.tableDBMapping();
            for (Map.Entry<String, RocksDBSessions> e : tableDBMap.entrySet()) {
                this.dropTable(e.getValue(), e.getKey());
            }

            LOG.debug("Store cleared: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    private void dropTable(RocksDBSessions db, String... tables) {
        try {
            db.dropTable(tables);
        } catch (BackendException e) {
            if (e.getMessage().contains("is not opened")) {
                return;
            }
            throw e;
        } catch (RocksDBException e) {
            throw new BackendException("Failed to drop tables %s for '%s'",
                                       e, Arrays.asList(tables), this.store);
        }
    }

    @Override
    public boolean initialized() {
        this.checkDbOpened();

        if (!this.opened()) {
            return false;
        }
        for (String table : this.tableNames()) {
            if (!this.sessions.existsTable(table)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void truncate() {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkOpened();

            this.clear(false);
            this.init();
            // clear write batch
            this.dbs.values().forEach(BackendSessionPool::forceResetSessions);
            LOG.debug("Store truncated: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void beginTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            for (Session session : this.session()) {
                assert !session.hasChanges();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void commitTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();
            // Unable to guarantee atomicity when committing multi sessions
            for (Session session : this.session()) {
                Object count = session.commit();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Store {} committed {} items", this.store, count);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void rollbackTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            for (Session session : this.session()) {
                session.rollback();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    protected Session session(HugeType tableType) {
        this.checkOpened();

        // Optimized disk
        String disk = this.tableDiskMapping.get(tableType);
        if (disk != null) {
            return this.db(disk).session();
        }

        return this.sessions.session();
    }

    @Override
    public Set<String> writeSnapshot(String snapshotPrefix) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            Set<String> uniqueParents = new HashSet<>();
            // Every rocksdb instance should create an snapshot
            for (Map.Entry<String, RocksDBSessions> entry : this.dbs.entrySet()) {
                // Like: parent_path/rocksdb-data/m
                //       parent_path/rocksdb-vertex/g
                Path originDataPath = Paths.get(entry.getKey());
                Path parentParentPath = originDataPath.getParent().getParent();
                // Like: rocksdb-data/m
                //       rocksdb-vertex/g
                Path pureDataPath = parentParentPath.relativize(originDataPath);
                // Like: parent_path/snapshot_rocksdb-data/m
                //       parent_path/snapshot_rocksdb-vertex/g
                Path snapshotPath = parentParentPath.resolve(snapshotPrefix +
                                                             "_" + pureDataPath);
                LOG.debug("The origin data path: {}", originDataPath);
                LOG.debug("The snapshot data path: {}", snapshotPath);
                RocksDBSessions sessions = entry.getValue();
                sessions.createSnapshot(snapshotPath.toString());

                uniqueParents.add(snapshotPath.getParent().toString());
            }
            LOG.info("The store '{}' save snapshot successfully", this.store);
            return uniqueParents;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void readSnapshot(String snapshotPrefix) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            if (!this.opened()) {
                return;
            }
            Map<Path, Path> fileNameMaps = new HashMap<>();
            for (Map.Entry<String, RocksDBSessions> entry : this.dbs.entrySet()) {
                // Like: parent_path/rocksdb-data/m
                //       parent_path/rocksdb-vertex/g
                Path originDataPath = Paths.get(entry.getKey());
                Path parentParentPath = originDataPath.getParent().getParent();
                // Like: rocksdb-data/m
                //       rocksdb-vertex/g
                Path pureDataPath = parentParentPath.relativize(originDataPath);
                // Like: parent_path/snapshot_rocksdb-data/m
                //       parent_path/snapshot_rocksdb-vertex/g
                Path snapshotPath = parentParentPath.resolve(snapshotPrefix +
                                                             "_" + pureDataPath);
                LOG.debug("The origin data path: {}", originDataPath);
                LOG.debug("The snapshot data path: {}", snapshotPath);
                fileNameMaps.put(originDataPath, snapshotPath);
            }
            E.checkState(!fileNameMaps.isEmpty(),
                         "The file name maps shouldn't be empty");
            /*
             * NOTE: must close rocksdb instance before deleting file directory,
             * if close after copying the snapshot directory to origin position,
             * it may produce dirty data.
             */
            for (RocksDBSessions sessions : this.sessions()) {
                sessions.forceCloseRocksDB();
            }
            // Move snapshot file to origin data path
            for (Map.Entry<Path, Path> entry : fileNameMaps.entrySet()) {
                File originDataDir = entry.getKey().toFile();
                File snapshotDir = entry.getValue().toFile();
                try {
                    if (originDataDir.exists()) {
                        LOG.info("Delete origin data directory {}",
                                 originDataDir);
                        FileUtils.deleteDirectory(originDataDir);
                    }
                    FileUtils.moveDirectory(snapshotDir, originDataDir);
                    LOG.info("Move snapshot directory {} to {}",
                             snapshotDir, originDataDir);
                } catch (IOException e) {
                    throw new BackendException("Failed to move %s to %s",
                                               e, snapshotDir, originDataDir);
                }
            }
            // Reload rocksdb instance
            for (RocksDBSessions sessions : this.sessions()) {
                sessions.reload();
            }
            LOG.info("The store '{}' load snapshot successfully", this.store);
        } catch (RocksDBException e) {
            throw new BackendException("Failed to reload rocksdb", e);
        } finally {
            readLock.unlock();
        }
    }

    private final void useSessions() {
        for (RocksDBSessions sessions : this.sessions()) {
            sessions.useSession();
        }
    }

    private final void closeSessions() {
        Iterator<Map.Entry<String, RocksDBSessions>> iter = this.dbs.entrySet()
                                                                    .iterator();
        while (iter.hasNext()) {
            Map.Entry<String, RocksDBSessions> entry = iter.next();
            RocksDBSessions sessions = entry.getValue();
            boolean closed = sessions.close();
            if (closed) {
                iter.remove();
            }
        }
    }

    private Session findMatchedSession(File snapshotFile) {
        String fileName = snapshotFile.getName();
        for (Session session : this.session()) {
            String md5 = GZipUtil.md5(session.dataPath());
            if (fileName.equals(md5)) {
                return session;
            }
        }
        throw new BackendException("Can't find matched session for " +
                                   "snapshot file %s", snapshotFile);
    }

    private final List<Session> session() {
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

    private final Collection<RocksDBSessions> sessions() {
        return this.dbs.values();
    }

    private final void parseTableDiskMapping(Map<String, String> disks,
                                             String dataPath) {
        this.tableDiskMapping.clear();
        for (Map.Entry<String, String> disk : disks.entrySet()) {
            // The format of `disk` like: `graph/vertex: /path/to/disk1`
            String name = disk.getKey();
            String path = disk.getValue();
            E.checkArgument(!dataPath.equals(path), "Invalid disk path" +
                            "(can't be the same as data_path): '%s'", path);
            E.checkArgument(!name.isEmpty() && !path.isEmpty(),
                            "Invalid disk format: '%s', expect `NAME:PATH`",
                            disk);
            String[] pair = name.split("/", 2);
            E.checkArgument(pair.length == 2,
                            "Invalid disk key format: '%s', " +
                            "expect `STORE/TABLE`", name);
            String store = pair[0].trim();
            HugeType table = HugeType.valueOf(pair[1].trim().toUpperCase());
            if (this.store.equals(store)) {
                path = this.wrapPath(path);
                this.tableDiskMapping.put(table, path);
            }
        }
    }

    private final void checkDbOpened() {
        E.checkState(this.sessions != null && !this.sessions.closed(),
                     "RocksDB has not been opened");
    }

    private RocksDBSessions db(String disk) {
        RocksDBSessions db = this.dbs.get(disk);
        E.checkState(db != null && !db.closed(),
                     "RocksDB store has not been opened: %s", disk);
        return db;
    }

    private static boolean existsOtherKeyspace(String dataPath) {
        Set<String> cfs;
        try {
            cfs = RocksDBStdSessions.listCFs(dataPath);
        } catch (RocksDBException e) {
            return false;
        }

        int matched = 0;
        for (String cf : cfs) {
            if (cf.endsWith(RocksDBTables.PropertyKey.TABLE) ||
                cf.endsWith(RocksDBTables.VertexLabel.TABLE) ||
                cf.endsWith(RocksDBTables.EdgeLabel.TABLE) ||
                cf.endsWith(RocksDBTables.IndexLabel.TABLE) ||
                cf.endsWith(RocksDBTables.SecondaryIndex.TABLE) ||
                cf.endsWith(RocksDBTables.SearchIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeIntIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeFloatIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeLongIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeDoubleIndex.TABLE)) {
                if (++matched >= 3) {
                    return true;
                }
            }
        }
        return false;
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSchemaStore extends RocksDBStore {

        private final RocksDBTables.Counters counters;

        public RocksDBSchemaStore(BackendStoreProvider provider,
                                  String database, String store) {
            super(provider, database, store);

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
        public void increaseCounter(HugeType type, long increment) {
            Lock readLock = super.storeLock.readLock();
            readLock.lock();
            try {
                super.checkOpened();
                Session session = super.sessions.session();
                this.counters.increaseCounter(session, type, increment);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public long getCounter(HugeType type) {
            Lock readLock = super.storeLock.readLock();
            readLock.lock();
            try {
                super.checkOpened();
                Session session = super.sessions.session();
                return this.counters.getCounter(session, type);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class RocksDBGraphStore extends RocksDBStore {

        public RocksDBGraphStore(BackendStoreProvider provider,
                                 String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                                 new RocksDBTables.Vertex(database));

            registerTableManager(HugeType.EDGE_OUT,
                                 RocksDBTables.Edge.out(database));
            registerTableManager(HugeType.EDGE_IN,
                                 RocksDBTables.Edge.in(database));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX,
                                 new RocksDBTables.VertexLabelIndex(database));
            registerTableManager(HugeType.EDGE_LABEL_INDEX,
                                 new RocksDBTables.EdgeLabelIndex(database));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new RocksDBTables.RangeIntIndex(database));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new RocksDBTables.RangeFloatIndex(database));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new RocksDBTables.RangeLongIndex(database));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new RocksDBTables.RangeDoubleIndex(database));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new RocksDBTables.SearchIndex(database));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new RocksDBTables.ShardIndex(database));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new RocksDBTables.UniqueIndex(database));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "RocksDBGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "RocksDBGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "RocksDBGraphStore.getCounter()");
        }
    }
}
