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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.Env;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileManager;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableList;

public class RocksDBStdSessions extends RocksDBSessions {

    private static final Logger LOG = Log.logger(RocksDBStdSessions.class);

    private final HugeConfig config;
    private final String dataPath;
    private final String walPath;

    private volatile OpenedRocksDB rocksdb;
    private final AtomicInteger refCount;

    public RocksDBStdSessions(HugeConfig config, String database, String store,
                              String dataPath, String walPath)
                              throws RocksDBException {
        super(config, database, store);
        this.config = config;
        this.dataPath = dataPath;
        this.walPath = walPath;
        this.rocksdb = RocksDBStdSessions.openRocksDB(config, dataPath,
                                                      walPath);
        this.refCount = new AtomicInteger(1);
    }

    public RocksDBStdSessions(HugeConfig config, String database, String store,
                              String dataPath, String walPath,
                              List<String> cfNames) throws RocksDBException {
        super(config, database, store);
        this.config = config;
        this.dataPath = dataPath;
        this.walPath = walPath;
        this.rocksdb = RocksDBStdSessions.openRocksDB(config, cfNames,
                                                      dataPath, walPath);
        this.refCount = new AtomicInteger(1);

        this.ingestExternalFile();
    }

    private RocksDBStdSessions(HugeConfig config, String database, String store,
                               RocksDBStdSessions origin) {
        super(config, database, store);
        this.config = config;
        this.dataPath = origin.dataPath;
        this.walPath = origin.walPath;
        this.rocksdb = origin.rocksdb;
        this.refCount = origin.refCount;
        this.refCount.incrementAndGet();
    }

    @Override
    public void open() throws Exception {
        // pass
    }

    @Override
    protected boolean opened() {
        return this.rocksdb != null && this.rocksdb.isOwningHandle();
    }

    @Override
    public Set<String> openedTables() {
        return this.rocksdb.cfs();
    }

    @Override
    public synchronized void createTable(String... tables)
                                         throws RocksDBException {
        this.checkValid();

        List<ColumnFamilyDescriptor> cfds = new ArrayList<>();
        for (String table : tables) {
            if (this.rocksdb.existCf(table)) {
                continue;
            }
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(
                                         encode(table));
            ColumnFamilyOptions options = cfd.getOptions();
            initOptions(this.config(), null, null, options, options);
            cfds.add(cfd);
        }

        /*
         * To speed up the creation of tables, like truncate() for tinkerpop
         * test, we call createColumnFamilies instead of createColumnFamily.
         */
        List<ColumnFamilyHandle> cfhs = this.rocksdb().createColumnFamilies(cfds);

        for (ColumnFamilyHandle cfh : cfhs) {
            String table = decode(cfh.getName());
            this.rocksdb.addCf(table, new CFHandle(cfh));
        }

        this.ingestExternalFile();
    }

    @Override
    public synchronized void dropTable(String... tables)
                                       throws RocksDBException {
        this.checkValid();

        /*
         * May cause bug to drop CF when someone is reading or writing this CF,
         * use CFHandle to wait for others and then do drop:
         * https://github.com/hugegraph/hugegraph/issues/697
         */
        List<ColumnFamilyHandle> cfhs = new ArrayList<>();
        for (String table : tables) {
            CFHandle cfh = this.rocksdb.cf(table);
            if (cfh == null) {
                continue;
            }
            cfhs.add(cfh.waitForDrop());
        }

        /*
         * To speed up the creation of tables, like truncate() for tinkerpop
         * test, we call dropColumnFamilies instead of dropColumnFamily.
         */
        this.rocksdb().dropColumnFamilies(cfhs);

        for (String table : tables) {
            CFHandle cfh = this.rocksdb.cf(table);
            if (cfh == null) {
                continue;
            }
            cfh.destroy();
            this.rocksdb.removeCf(table);
        }
    }

    @Override
    public boolean existsTable(String table) {
        return this.rocksdb.existCf(table);
    }

    @Override
    public void reloadRocksDB() throws RocksDBException {
        if (this.rocksdb.isOwningHandle()) {
            this.rocksdb.close();
        }
        this.rocksdb = RocksDBStdSessions.openRocksDB(this.config,
                                                      ImmutableList.of(),
                                                      this.dataPath,
                                                      this.walPath);
    }

    @Override
    public void forceCloseRocksDB() {
        this.rocksdb().close();
    }

    @Override
    public List<String> property(String property) {
        try {
            if (property.equals(RocksDBMetrics.DISK_USAGE)) {
                long size = this.rocksdb.sstFileManager.getTotalSize();
                return ImmutableList.of(String.valueOf(size));
            }
            List<String> values = new ArrayList<>();
            for (String cf : this.openedTables()) {
                try (CFHandle cfh = this.cf(cf)) {
                    values.add(this.rocksdb().getProperty(cfh.get(), property));
                }
            }
            return values;
        } catch(RocksDBException | UnsupportedOperationException e) {
            throw new BackendException(e);
        }
    }

    @Override
    public RocksDBSessions copy(HugeConfig config,
                                String database, String store) {
        return new RocksDBStdSessions(config, database, store, this);
    }

    @Override
    public void createSnapshot(String snapshotPath) {
        RocksDBStdSessions.createCheckpoint(this.rocksdb(), snapshotPath);
    }

    @Override
    public void resumeSnapshot(String snapshotPath) {
        File originDataDir = new File(this.dataPath);
        File snapshotDir = new File(snapshotPath);
        try {
            /*
             * Close current instance first
             * NOTE: must close rocksdb instance before deleting file directory,
             * if close after copying the snapshot directory to origin position,
             * it may produce dirty data.
             */
            this.forceCloseRocksDB();
            // Delete origin data directory
            if (originDataDir.exists()) {
                LOG.info("Delete origin data directory {}", originDataDir);
                FileUtils.deleteDirectory(originDataDir);
            }
            // Move snapshot directory to origin data directory
            FileUtils.moveDirectory(snapshotDir, originDataDir);
            LOG.info("Move snapshot directory {} to {}",
                     snapshotDir, originDataDir);
            // Reload rocksdb instance
            this.reloadRocksDB();
        } catch (Exception e) {
            throw new BackendException("Failed to resume snapshot '%s' to' %s'",
                                       e, snapshotDir, this.dataPath);
        }
    }

    @Override
    public String buildSnapshotPath(String snapshotPrefix) {
        // Like: parent_path/rocksdb-data/*, * can be g,m,s
        Path originDataPath = Paths.get(this.dataPath);
        Path parentParentPath = originDataPath.getParent().getParent();
        // Like: rocksdb-data/*
        Path pureDataPath = parentParentPath.relativize(originDataPath);
        // Like: parent_path/snapshot_rocksdb-data/*
        Path snapshotPath = parentParentPath.resolve(snapshotPrefix + "_" +
                                                     pureDataPath);
        E.checkArgument(snapshotPath.toFile().exists(),
                        "The snapshot path '%s' doesn't exist",
                        snapshotPath);
        return snapshotPath.toString();
    }

    @Override
    public String hardLinkSnapshot(String snapshotPath) throws RocksDBException {
        String snapshotLinkPath = this.dataPath + "_temp";
        try (RocksDB rocksdb = openRocksDB(this.config, ImmutableList.of(),
                                           snapshotPath, null).rocksdb) {
            RocksDBStdSessions.createCheckpoint(rocksdb, snapshotLinkPath);
        }
        LOG.info("The snapshot {} has been hard linked to {}",
                 snapshotPath, snapshotLinkPath);
        return snapshotLinkPath;
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final Session newSession() {
        E.checkState(this.rocksdb.isOwningHandle(),
                     "RocksDB has not been initialized");
        return new StdSession(this.config());
    }

    @Override
    protected synchronized void doClose() {
        this.checkValid();

        if (this.refCount.decrementAndGet() > 0) {
            return;
        }
        assert this.refCount.get() == 0;
        this.rocksdb.close();
    }

    private void checkValid() {
        E.checkState(this.rocksdb.isOwningHandle(),
                     "It seems RocksDB has been closed");
    }

    private RocksDB rocksdb() {
        this.checkValid();
        return this.rocksdb.rocksdb;
    }

    private CFHandle cf(String cfName) {
        CFHandle cfh = this.rocksdb.cf(cfName);
        if (cfh == null) {
            throw new BackendException("Table '%s' is not opened", cfName);
        }
        cfh.open();
        return cfh;
    }

    private void ingestExternalFile() throws RocksDBException {
        String directory = this.config().get(RocksDBOptions.SST_PATH);
        if (directory == null || directory.isEmpty()) {
            return;
        }
        RocksDBIngester ingester = new RocksDBIngester(this.rocksdb());
        // Ingest all *.sst files in `directory`
        for (String cf : this.rocksdb.cfs()) {
            Path path = Paths.get(directory, cf);
            if (path.toFile().isDirectory()) {
                try (CFHandle cfh = this.cf(cf)) {
                    ingester.ingest(path, cfh.get());
                }
            }
        }
    }

    private static OpenedRocksDB openRocksDB(HugeConfig config,
                                             String dataPath, String walPath)
                                             throws RocksDBException {
        // Init options
        Options options = new Options();
        RocksDBStdSessions.initOptions(config, options, options,
                                       options, options);
        options.setWalDir(walPath);
        SstFileManager sstFileManager = new SstFileManager(Env.getDefault());
        options.setSstFileManager(sstFileManager);
        /*
         * Open RocksDB at the first time
         * Don't merge old CFs, we expect a clear DB when using this one
         */
        RocksDB rocksdb = RocksDB.open(options, dataPath);
        Map<String, CFHandle> cfs = new ConcurrentHashMap<>();
        return new OpenedRocksDB(rocksdb, cfs, sstFileManager);
    }

    private static OpenedRocksDB openRocksDB(HugeConfig config,
                                             List<String> cfNames,
                                             String dataPath, String walPath)
                                             throws RocksDBException {
        // Old CFs should always be opened
        Set<String> mergedCFs = RocksDBStdSessions.mergeOldCFs(dataPath,
                                                               cfNames);
        List<String> cfs = ImmutableList.copyOf(mergedCFs);

        // Init CFs options
        List<ColumnFamilyDescriptor> cfds = new ArrayList<>(cfs.size());
        for (String cf : cfs) {
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encode(cf));
            ColumnFamilyOptions options = cfd.getOptions();
            RocksDBStdSessions.initOptions(config, null, null,
                                           options, options);
            cfds.add(cfd);
        }

        // Init DB options
        DBOptions options = new DBOptions();
        RocksDBStdSessions.initOptions(config, options, options, null, null);
        if (walPath != null) {
            options.setWalDir(walPath);
        }
        SstFileManager sstFileManager = new SstFileManager(Env.getDefault());
        options.setSstFileManager(sstFileManager);

        // Open RocksDB with CFs
        List<ColumnFamilyHandle> cfhs = new ArrayList<>();
        RocksDB rocksdb = RocksDB.open(options, dataPath, cfds, cfhs);

        E.checkState(cfhs.size() == cfs.size(),
                     "Expect same size of cf-handles and cf-names");
        // Collect CF Handles
        Map<String, CFHandle> cfHandles = new ConcurrentHashMap<>();
        for (int i = 0; i < cfs.size(); i++) {
            cfHandles.put(cfs.get(i), new CFHandle(cfhs.get(i)));
        }
        return new OpenedRocksDB(rocksdb, cfHandles, sstFileManager);
    }

    private static Set<String> mergeOldCFs(String path, List<String> cfNames)
                                           throws RocksDBException {
        Set<String> cfs = listCFs(path);
        cfs.addAll(cfNames);
        return cfs;
    }

    private static void createCheckpoint(RocksDB rocksdb, String targetPath) {
        Path parentPath = Paths.get(targetPath).getParent().getFileName();
        assert parentPath.toString().startsWith("snapshot") : targetPath;
        // https://github.com/facebook/rocksdb/wiki/Checkpoints
        try (Checkpoint checkpoint = Checkpoint.create(rocksdb)) {
            String tempPath = targetPath + "_temp";
            File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            LOG.debug("Deleted temp directory {}", tempFile);

            FileUtils.forceMkdir(tempFile.getParentFile());
            checkpoint.createCheckpoint(tempPath);
            File snapshotFile = new File(targetPath);
            FileUtils.deleteDirectory(snapshotFile);
            LOG.debug("Deleted stale directory {}", snapshotFile);
            if (!tempFile.renameTo(snapshotFile)) {
                throw new IOException(String.format("Failed to rename %s to %s",
                                                    tempFile, snapshotFile));
            }
        } catch (Exception e) {
            throw new BackendException("Failed to create checkpoint at path %s",
                                       e, targetPath);
        }
    }

    public static Set<String> listCFs(String path) throws RocksDBException {
        Set<String> cfs = new HashSet<>();

        List<byte[]> oldCFs = RocksDB.listColumnFamilies(new Options(), path);
        if (oldCFs.isEmpty()) {
            cfs.add("default");
        } else {
            for (byte[] oldCF : oldCFs) {
                cfs.add(decode(oldCF));
            }
        }
        return cfs;
    }

    public static void initOptions(HugeConfig conf,
                                   DBOptionsInterface<?> db,
                                   MutableDBOptionsInterface<?> mdb,
                                   ColumnFamilyOptionsInterface<?> cf,
                                   MutableColumnFamilyOptionsInterface<?> mcf) {
        final boolean optimize = conf.get(RocksDBOptions.OPTIMIZE_MODE);

        if (db != null) {
            /*
             * Set true then the database will be created if it is missing.
             * should we use options.setCreateMissingColumnFamilies()?
             */
            db.setCreateIfMissing(true);

            // Optimize RocksDB
            if (optimize) {
                int processors = CoreOptions.CPUS;
                db.setIncreaseParallelism(Math.max(processors / 2, 1));

                db.setAllowConcurrentMemtableWrite(true);
                db.setEnableWriteThreadAdaptiveYield(true);
            }
            db.setInfoLogLevel(InfoLogLevel.valueOf(
                    conf.get(RocksDBOptions.LOG_LEVEL) + "_LEVEL"));

            db.setMaxSubcompactions(
                    conf.get(RocksDBOptions.MAX_SUB_COMPACTIONS));

            db.setAllowMmapWrites(
                    conf.get(RocksDBOptions.ALLOW_MMAP_WRITES));
            db.setAllowMmapReads(
                    conf.get(RocksDBOptions.ALLOW_MMAP_READS));

            db.setUseDirectReads(
                    conf.get(RocksDBOptions.USE_DIRECT_READS));
            db.setUseDirectIoForFlushAndCompaction(
                    conf.get(RocksDBOptions.USE_DIRECT_READS_WRITES_FC));

            db.setMaxManifestFileSize(
                    conf.get(RocksDBOptions.MAX_MANIFEST_FILE_SIZE));

            db.setSkipStatsUpdateOnDbOpen(
                    conf.get(RocksDBOptions.SKIP_STATS_UPDATE_ON_DB_OPEN));

            db.setMaxFileOpeningThreads(
                    conf.get(RocksDBOptions.MAX_FILE_OPENING_THREADS));

            db.setDbWriteBufferSize(conf.get(RocksDBOptions.DB_MEMTABLE_SIZE));
        }

        if (mdb != null) {
            /*
             * Migrate to max_background_jobs option
             * https://github.com/facebook/rocksdb/wiki/Thread-Pool
             * https://github.com/facebook/rocksdb/pull/2205/files
             */
            mdb.setMaxBackgroundJobs(conf.get(RocksDBOptions.MAX_BG_JOBS));

            mdb.setDelayedWriteRate(
                    conf.get(RocksDBOptions.DELAYED_WRITE_RATE));

            mdb.setMaxOpenFiles(conf.get(RocksDBOptions.MAX_OPEN_FILES));

            mdb.setMaxTotalWalSize(conf.get(RocksDBOptions.MAX_TOTAL_WAL_SIZE));

            mdb.setDeleteObsoleteFilesPeriodMicros(1000000 *
                    conf.get(RocksDBOptions.DELETE_OBSOLETE_FILE_PERIOD));
        }

        if (cf != null) {
            if (optimize) {
                // Optimize RocksDB
                cf.optimizeLevelStyleCompaction();
                cf.optimizeUniversalStyleCompaction();
            }

            int numLevels = conf.get(RocksDBOptions.NUM_LEVELS);
            List<CompressionType> compressions = conf.get(
                                  RocksDBOptions.LEVELS_COMPRESSIONS);
            E.checkArgument(compressions.isEmpty() ||
                            compressions.size() == numLevels,
                            "Elements number of '%s' must be 0 or " +
                            "be the same as '%s', bug got %s != %s",
                            RocksDBOptions.LEVELS_COMPRESSIONS.name(),
                            RocksDBOptions.NUM_LEVELS.name(),
                            compressions.size(), numLevels);

            cf.setNumLevels(numLevels);
            cf.setCompactionStyle(conf.get(RocksDBOptions.COMPACTION_STYLE));

            cf.setBottommostCompressionType(
                    conf.get(RocksDBOptions.BOTTOMMOST_COMPRESSION));
            if (!compressions.isEmpty()) {
                cf.setCompressionPerLevel(compressions);
            }

            cf.setMinWriteBufferNumberToMerge(
                    conf.get(RocksDBOptions.MIN_MEMTABLES_TO_MERGE));
            cf.setMaxWriteBufferNumberToMaintain(
                    conf.get(RocksDBOptions.MAX_MEMTABLES_TO_MAINTAIN));

            cf.setLevelCompactionDynamicLevelBytes(
                    conf.get(RocksDBOptions.DYNAMIC_LEVEL_BYTES));

            // https://github.com/facebook/rocksdb/wiki/Block-Cache
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            long cacheCapacity = conf.get(RocksDBOptions.BLOCK_CACHE_CAPACITY);
            if (cacheCapacity <= 0L) {
                // Bypassing bug https://github.com/facebook/rocksdb/pull/5465
                tableConfig.setNoBlockCache(true);
            } else {
                tableConfig.setBlockCache(new LRUCache(cacheCapacity));
            }
            tableConfig.setPinL0FilterAndIndexBlocksInCache(
                    conf.get(RocksDBOptions.PIN_L0_FILTER_AND_INDEX_IN_CACHE));
            tableConfig.setCacheIndexAndFilterBlocks(
                    conf.get(RocksDBOptions.PUT_FILTER_AND_INDEX_IN_CACHE));

            // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
            int bitsPerKey = conf.get(RocksDBOptions.BLOOM_FILTER_BITS_PER_KEY);
            if (bitsPerKey >= 0) {
                boolean blockBased = conf.get(RocksDBOptions.BLOOM_FILTER_MODE);
                tableConfig.setFilterPolicy(new BloomFilter(bitsPerKey,
                                                            blockBased));
            }
            tableConfig.setWholeKeyFiltering(
                    conf.get(RocksDBOptions.BLOOM_FILTER_WHOLE_KEY));
            cf.setTableFormatConfig(tableConfig);

            cf.setOptimizeFiltersForHits(
                    conf.get(RocksDBOptions.BLOOM_FILTERS_SKIP_LAST_LEVEL));

            // https://github.com/facebook/rocksdb/tree/master/utilities/merge_operators
            cf.setMergeOperatorName("uint64add"); // uint64add/stringappend
        }

        if (mcf != null) {
            mcf.setCompressionType(conf.get(RocksDBOptions.COMPRESSION));

            mcf.setWriteBufferSize(conf.get(RocksDBOptions.MEMTABLE_SIZE));
            mcf.setMaxWriteBufferNumber(conf.get(RocksDBOptions.MAX_MEMTABLES));

            mcf.setMaxBytesForLevelBase(
                    conf.get(RocksDBOptions.MAX_LEVEL1_BYTES));
            mcf.setMaxBytesForLevelMultiplier(
                    conf.get(RocksDBOptions.MAX_LEVEL_BYTES_MULTIPLIER));

            mcf.setTargetFileSizeBase(
                    conf.get(RocksDBOptions.TARGET_FILE_SIZE_BASE));
            mcf.setTargetFileSizeMultiplier(
                    conf.get(RocksDBOptions.TARGET_FILE_SIZE_MULTIPLIER));

            mcf.setLevel0FileNumCompactionTrigger(
                    conf.get(RocksDBOptions.LEVEL0_COMPACTION_TRIGGER));
            mcf.setLevel0SlowdownWritesTrigger(
                    conf.get(RocksDBOptions.LEVEL0_SLOWDOWN_WRITES_TRIGGER));
            mcf.setLevel0StopWritesTrigger(
                    conf.get(RocksDBOptions.LEVEL0_STOP_WRITES_TRIGGER));

            mcf.setSoftPendingCompactionBytesLimit(
                    conf.get(RocksDBOptions.SOFT_PENDING_COMPACTION_LIMIT));
            mcf.setHardPendingCompactionBytesLimit(
                    conf.get(RocksDBOptions.HARD_PENDING_COMPACTION_LIMIT));

            boolean bulkload = conf.get(RocksDBOptions.BULKLOAD_MODE);
            if (bulkload) {
                // Disable automatic compaction
                mcf.setDisableAutoCompactions(true);

                int trigger = Integer.MAX_VALUE;
                mcf.setLevel0FileNumCompactionTrigger(trigger);
                mcf.setLevel0SlowdownWritesTrigger(trigger);
                mcf.setLevel0StopWritesTrigger(trigger);

                long limit = Long.MAX_VALUE;
                mcf.setSoftPendingCompactionBytesLimit(limit);
                mcf.setHardPendingCompactionBytesLimit(limit);

                //cf.setMemTableConfig(new VectorMemTableConfig());
            }
        }
    }

    public static final byte[] encode(String string) {
        return StringEncoding.encode(string);
    }

    public static final String decode(byte[] bytes) {
        return StringEncoding.decode(bytes);
    }

    private static class OpenedRocksDB {

        private final RocksDB rocksdb;
        private final Map<String, CFHandle> cfHandles;
        private final SstFileManager sstFileManager;

        public OpenedRocksDB(RocksDB rocksdb, Map<String, CFHandle> cfHandles,
                             SstFileManager sstFileManager) {
            this.rocksdb = rocksdb;
            this.cfHandles = cfHandles;
            this.sstFileManager = sstFileManager;
        }

        public Set<String> cfs() {
            return this.cfHandles.keySet();
        }

        public CFHandle cf(String cfName) {
            return this.cfHandles.get(cfName);
        }

        public void addCf(String cfName, CFHandle cfHandle) {
            this.cfHandles.put(cfName, cfHandle);
        }

        public CFHandle removeCf(String cfName) {
            return this.cfHandles.remove(cfName);
        }

        public boolean existCf(String cfName) {
            return this.cfHandles.containsKey(cfName);
        }

        public boolean isOwningHandle() {
            return this.rocksdb.isOwningHandle();
        }

        public void close() {
            if (!this.isOwningHandle()) {
                return;
            }
            for (CFHandle cf : this.cfHandles.values()) {
                cf.close();
            }
            this.cfHandles.clear();

            this.rocksdb.close();
        }
    }

    private static class CFHandle implements Closeable {

        private final ColumnFamilyHandle handle;
        private final AtomicInteger refs;

        public CFHandle(ColumnFamilyHandle handle) {
            E.checkNotNull(handle, "handle");
            this.handle = handle;
            this.refs = new AtomicInteger(1);
        }

        public synchronized ColumnFamilyHandle get() {
            E.checkState(this.handle.isOwningHandle(),
                         "It seems CF has been closed");
            return this.handle;
        }

        public synchronized void open() {
            this.refs.incrementAndGet();
        }

        @Override
        public void close() {
            if (this.refs.decrementAndGet() <= 0) {
                this.handle.close();
            }
        }

        public synchronized ColumnFamilyHandle waitForDrop() {
            assert this.refs.get() >= 1;
            // When entering this method, the refs won't increase any more
            final long timeout = TimeUnit.MINUTES.toMillis(30L);
            final long unit = 100L;
            for (long i = 1; this.refs.get() > 1; i++) {
                try {
                    Thread.sleep(unit);
                } catch (InterruptedException ignored) {
                    // 30s rest api timeout may cause InterruptedException
                }
                if (i * unit > timeout) {
                    throw new BackendException("Timeout after %sms to drop CF",
                                               timeout);
                }
            }
            assert this.refs.get() == 1;
            return this.handle;
        }

        public synchronized void destroy() {
            this.close();
            assert this.refs.get() == 0 && !this.handle.isOwningHandle();
        }
    }

    /**
     * StdSession implement for RocksDB
     */
    private final class StdSession extends RocksDBSessions.Session {

        private WriteBatch batch;
        private WriteOptions writeOptions;

        public StdSession(HugeConfig conf) {
            boolean raftMode = conf.get(CoreOptions.RAFT_MODE);
            this.batch = new WriteBatch();
            this.writeOptions = new WriteOptions();
            /*
             * When work under raft mode. if store crashed, the state-machine
             * can restore by snapshot + raft log, doesn't need wal and sync
             */
            if (raftMode) {
                this.writeOptions.setDisableWAL(true);
                this.writeOptions.setSync(false);
            }
        }

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            assert this.closeable();
            this.opened = false;
        }

        @Override
        public boolean closed() {
            return !this.opened || !RocksDBStdSessions.this.opened();
        }

        @Override
        public void reset() {
            this.batch = new WriteBatch();
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.batch.count() > 0;
        }

        @Override
        public String dataPath() {
            return RocksDBStdSessions.this.dataPath;
        }

        @Override
        public String walPath() {
            return RocksDBStdSessions.this.walPath;
        }

        /**
         * Get property value by name from specified table
         */
        @Override
        public String property(String table, String property) {
            try (CFHandle cf = cf(table)) {
                return rocksdb().getProperty(cf.get(), property);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            byte[] startKey, endKey;
            try (CFHandle cf = cf(table);
                 RocksIterator iter = rocksdb().newIterator(cf.get())) {
                iter.seekToFirst();
                if (!iter.isValid()) {
                    return null;
                }
                startKey = iter.key();
                iter.seekToLast();
                if (!iter.isValid()) {
                    return Pair.of(startKey, null);
                }
                endKey = iter.key();
            }
            return Pair.of(startKey, endKey);
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {
            int count = this.batch.count();
            if (count <= 0) {
                return 0;
            }

            try {
                rocksdb().write(this.writeOptions, this.batch);
            } catch (RocksDBException e) {
                //this.batch.rollbackToSavePoint();
                throw new BackendException(e);
            }

            // Clear batch if write() successfully (retained if failed)
            this.batch.clear();

            return count;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            this.batch.clear();
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] key, byte[] value) {
            try (CFHandle cf = cf(table)) {
                this.batch.put(cf.get(), key, value);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Merge a record to an existing key to a table
         * For more details about merge-operator:
         *  https://github.com/facebook/rocksdb/wiki/merge-operator
         */
        @Override
        public void merge(String table, byte[] key, byte[] value) {
            try (CFHandle cf = cf(table)) {
                this.batch.merge(cf.get(), key, value);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Merge a record to an existing key to a table and commit immediately
         */
        @Override
        public void increase(String table, byte[] key, byte[] value) {
            try (CFHandle cf = cf(table)) {
                rocksdb().merge(cf.get(), key, value);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Delete a record by key from a table
         */
        @Override
        public void delete(String table, byte[] key) {
            try (CFHandle cf = cf(table)) {
                this.batch.delete(cf.get(), key);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Delete the only one version of a record by key from a table
         * NOTE: requires that the key exists and was not overwritten.
         */
        @Override
        public void deleteSingle(String table, byte[] key) {
            try (CFHandle cf = cf(table)) {
                this.batch.singleDelete(cf.get(), key);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Delete a record by key(or prefix with key) from a table
         */
        @Override
        public void deletePrefix(String table, byte[] key) {
            byte[] keyFrom = key;
            byte[] keyTo = Arrays.copyOf(key, key.length);
            keyTo = BinarySerializer.increaseOne(keyTo);
            try (CFHandle cf = cf(table)) {
                this.batch.deleteRange(cf.get(), keyFrom, keyTo);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void deleteRange(String table, byte[] keyFrom, byte[] keyTo) {
            try (CFHandle cf = cf(table)) {
                this.batch.deleteRange(cf.get(), keyFrom, keyTo);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Get a record by key from a table
         */
        @Override
        public byte[] get(String table, byte[] key) {
            assert !this.hasChanges();

            try (CFHandle cf = cf(table)) {
                return rocksdb().get(cf.get(), key);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Scan all records from a table
         */
        @Override
        public BackendColumnIterator scan(String table) {
            assert !this.hasChanges();
            try (CFHandle cf = cf(table)) {
                RocksIterator iter = rocksdb().newIterator(cf.get());
                return new ColumnIterator(table, iter, null, null, SCAN_ANY);
            }
        }

        /**
         * Scan records by key prefix from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            /*
             * NOTE: Options.prefix_extractor is a prerequisite for
             * Options.setPrefixSameAsStart(true):
             * ReadOptions options = new ReadOptions();
             * options.setPrefixSameAsStart(true);
             */
            try (CFHandle cf = cf(table)) {
                RocksIterator iter = rocksdb().newIterator(cf.get());
                return new ColumnIterator(table, iter, prefix, null,
                                          SCAN_PREFIX_BEGIN);
            }
        }

        /**
         * Scan records by key range from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            assert !this.hasChanges();
            /*
             * Not sure if setTotalOrderSeek(true) must be set:
             * ReadOptions options = new ReadOptions();
             * options.setTotalOrderSeek(true);
             */
            try (CFHandle cf = cf(table)) {
                RocksIterator iter = rocksdb().newIterator(cf.get());
                return new ColumnIterator(table, iter, keyFrom,
                                          keyTo, scanType);
            }
        }
    }

    /**
     * A wrapper for RocksIterator that convert RocksDB results to std Iterator
     */
    private static class ColumnIterator implements BackendColumnIterator,
                                                   Countable {

        private final String table;
        private final RocksIterator iter;
        private final byte[] keyBegin;
        private final byte[] keyEnd;
        private final int scanType;

        private byte[] position;
        private boolean matched;

        public ColumnIterator(String table, RocksIterator iter,
                              byte[] keyBegin, byte[] keyEnd, int scanType) {
            E.checkNotNull(iter, "iter");
            this.table = table;

            this.iter = iter;
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.scanType = scanType;

            this.position = keyBegin;
            this.matched = false;

            this.checkArguments();

            //this.dump();

            this.seek();
        }

        private void checkArguments() {
            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
                              this.match(Session.SCAN_PREFIX_END)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_PREFIX_WITH_END at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
                              this.match(Session.SCAN_GT_BEGIN)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_GT_BEGIN/SCAN_GTE_BEGIN at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_END) &&
                              this.match(Session.SCAN_LT_END)),
                            "Can't set SCAN_PREFIX_WITH_END and " +
                            "SCAN_LT_END/SCAN_LTE_END at the same time");

            if (this.match(Session.SCAN_PREFIX_BEGIN)) {
                E.checkArgument(this.keyBegin != null,
                                "Parameter `keyBegin` can't be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
                E.checkArgument(this.keyEnd == null,
                                "Parameter `keyEnd` must be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
            }

            if (this.match(Session.SCAN_PREFIX_END)) {
                E.checkArgument(this.keyEnd != null,
                                "Parameter `keyEnd` can't be null " +
                                "if set SCAN_PREFIX_WITH_END");
            }

            if (this.match(Session.SCAN_GT_BEGIN)) {
                E.checkArgument(this.keyBegin != null,
                                "Parameter `keyBegin` can't be null " +
                                "if set SCAN_GT_BEGIN or SCAN_GTE_BEGIN");
            }

            if (this.match(Session.SCAN_LT_END)) {
                E.checkArgument(this.keyEnd != null,
                                "Parameter `keyEnd` can't be null " +
                                "if set SCAN_LT_END or SCAN_LTE_END");
            }
        }

        private boolean match(int expected) {
            return Session.matchScanType(expected, this.scanType);
        }

        /**
         * Just for debug
         */
        @SuppressWarnings("unused")
        private void dump() {
            this.seek();
            System.out.println(">>>> scan from " + this.table + ": "  +
                               (this.keyBegin == null ? "*" :
                                StringEncoding.format(this.keyBegin)) +
                               (this.iter.isValid() ? "" : " - No data"));
            for (; this.iter.isValid(); this.iter.next()) {
                System.out.println(String.format("%s=%s",
                                   StringEncoding.format(this.iter.key()),
                                   StringEncoding.format(this.iter.value())));
            }
        }

        @Override
        public boolean hasNext() {
            this.matched = this.iter.isOwningHandle();
            if (!this.matched) {
                // Maybe closed
                return this.matched;
            }

            this.matched = this.iter.isValid();
            if (this.matched) {
                // Update position for paging
                this.position = this.iter.key();
                // Do filter if not SCAN_ANY
                if (!this.match(Session.SCAN_ANY)) {
                    this.matched = this.filter(this.position);
                }
            }
            if (!this.matched) {
                // The end
                this.position = null;
                // Free the iterator if finished
                this.close();
            }
            return this.matched;
        }

        private void seek() {
            if (this.keyBegin == null) {
                // Seek to the first if no `keyBegin`
                this.iter.seekToFirst();
            } else {
                /*
                 * Seek to `keyBegin`:
                 * if set SCAN_GT_BEGIN/SCAN_GTE_BEGIN (key > / >= 'xx')
                 * or if set SCAN_PREFIX_WITH_BEGIN (key prefix with 'xx')
                 */
                this.iter.seek(this.keyBegin);

                // Skip `keyBegin` if set SCAN_GT_BEGIN (key > 'xx')
                if (this.match(Session.SCAN_GT_BEGIN) &&
                    !this.match(Session.SCAN_GTE_BEGIN)) {
                    while (this.iter.isValid() &&
                           Bytes.equals(this.iter.key(), this.keyBegin)) {
                        this.iter.next();
                    }
                }
            }
        }

        private boolean filter(byte[] key) {
            if (this.match(Session.SCAN_PREFIX_BEGIN)) {
                /*
                 * Prefix with `keyBegin`?
                 * TODO: use custom prefix_extractor instead
                 *       or use ReadOptions.prefix_same_as_start
                 */
                return Bytes.prefixWith(key, this.keyBegin);
            } else if (this.match(Session.SCAN_PREFIX_END)) {
                /*
                 * Prefix with `keyEnd`?
                 * like the following query for range index:
                 *  key > 'age:20' and prefix with 'age'
                 */
                assert this.keyEnd != null;
                return Bytes.prefixWith(key, this.keyEnd);
            } else if (this.match(Session.SCAN_LT_END)) {
                /*
                 * Less (equal) than `keyEnd`?
                 * NOTE: don't use BytewiseComparator due to signed byte
                 */
                assert this.keyEnd != null;
                if (this.match(Session.SCAN_LTE_END)) {
                    // Just compare the prefix, can be there are excess tail
                    key = Arrays.copyOfRange(key, 0, this.keyEnd.length);
                    return Bytes.compare(key, this.keyEnd) <= 0;
                } else {
                    return Bytes.compare(key, this.keyEnd) < 0;
                }
            } else {
                assert this.match(Session.SCAN_ANY) ||
                       this.match(Session.SCAN_GT_BEGIN) ||
                       this.match(Session.SCAN_GTE_BEGIN) :
                       "Unknow scan type";
                return true;
            }
        }

        @Override
        public BackendColumn next() {
            if (!this.matched) {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
            }

            BackendColumn col = BackendColumn.of(this.iter.key(),
                                                 this.iter.value());
            this.iter.next();
            this.matched = false;

            return col;
        }

        @Override
        public long count() {
            long count = 0L;
            while (this.hasNext()) {
                this.iter.next();
                this.matched = false;
                count++;
                BackendEntryIterator.checkInterrupted();
            }
            return count;
        }

        @Override
        public byte[] position() {
            return this.position;
        }

        @Override
        public void close() {
            if (this.iter.isOwningHandle()) {
                this.iter.close();
            }
        }
    }
}
