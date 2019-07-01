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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.Env;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileManager;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableList;

public class RocksDBStdSessions extends RocksDBSessions {

    private final RocksDB rocksdb;
    private final SstFileManager sstFileManager;

    private final Map<String, ColumnFamilyHandle> cfs;
    private final AtomicInteger refCount;

    public RocksDBStdSessions(HugeConfig config, String database, String store,
                              String dataPath,String walPath)
                              throws RocksDBException {
        super(config, database, store);

        // Init options
        Options options = new Options();
        RocksDBStdSessions.initOptions(config, options, options, options);
        options.setWalDir(walPath);

        this.sstFileManager = new SstFileManager(Env.getDefault());
        options.setSstFileManager(this.sstFileManager);

        /*
         * Open RocksDB at the first time
         * Don't merge old CFs, we expect a clear DB when using this one
         */
        this.rocksdb = RocksDB.open(options, dataPath);

        this.cfs = new HashMap<>();
        this.refCount = new AtomicInteger(1);
    }

    public RocksDBStdSessions(HugeConfig config, String database, String store,
                              String dataPath, String walPath,
                              List<String> cfNames) throws RocksDBException {
        super(config, database, store);

        // Old CFs should always be opened
        Set<String> mergedCFs = this.mergeOldCFs(dataPath, cfNames);
        List<String> cfs = ImmutableList.copyOf(mergedCFs);

        // Init CFs options
        List<ColumnFamilyDescriptor> cfds = new ArrayList<>(cfs.size());
        for (String cf : cfs) {
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encode(cf));
            ColumnFamilyOptions options = cfd.getOptions();
            RocksDBStdSessions.initOptions(config, null, options, options);
            cfds.add(cfd);
        }

        // Init DB options
        DBOptions options = new DBOptions();
        RocksDBStdSessions.initOptions(config, options, null, null);
        options.setWalDir(walPath);

        this.sstFileManager = new SstFileManager(Env.getDefault());
        options.setSstFileManager(this.sstFileManager);

        // Open RocksDB with CFs
        List<ColumnFamilyHandle> cfhs = new ArrayList<>();
        this.rocksdb = RocksDB.open(options, dataPath, cfds, cfhs);
        E.checkState(cfhs.size() == cfs.size(),
                     "Expect same size of cf-handles and cf-names");

        // Collect CF Handles
        this.cfs = new HashMap<>();
        for (int i = 0; i < cfs.size(); i++) {
            this.cfs.put(cfs.get(i), cfhs.get(i));
        }

        this.refCount = new AtomicInteger(1);

        ingestExternalFile();
    }

    private RocksDBStdSessions(HugeConfig config, String database, String store,
                               RocksDBStdSessions origin) {
        super(config, database, store);

        this.rocksdb = origin.rocksdb;
        this.sstFileManager = origin.sstFileManager;
        this.cfs = origin.cfs;
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
        return this.cfs.keySet();
    }

    @Override
    public void createTable(String table) throws RocksDBException {
        if (this.cfs.containsKey(table)) {
            return;
        }

        this.checkValid();

        // Should we use options.setCreateMissingColumnFamilies() to create CF
        ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encode(table));
        ColumnFamilyOptions options = cfd.getOptions();
        initOptions(this.config(), null, options, options);
        this.cfs.put(table, this.rocksdb.createColumnFamily(cfd));

        ingestExternalFile();
    }

    @Override
    public void dropTable(String table) throws RocksDBException {
        this.checkValid();

        ColumnFamilyHandle cfh = cf(table);
        this.rocksdb.dropColumnFamily(cfh);
        cfh.close();
        this.cfs.remove(table);
    }

    @Override
    public String property(String property) {
        try {
            if (property.equals(RocksDBMetrics.DISK_USAGE)) {
                return String.valueOf(this.sstFileManager.getTotalSize());
            }
            return rocksdb().getProperty(property);
        } catch (RocksDBException e) {
            throw new BackendException(e);
        }
    }

    @Override
    public RocksDBSessions copy(HugeConfig config,
                                String database, String store) {
        return new RocksDBStdSessions(config, database, store, this);
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

        for (ColumnFamilyHandle cf : this.cfs.values()) {
            cf.close();
        }
        this.cfs.clear();

        this.rocksdb.close();
    }

    private void checkValid() {
        E.checkState(this.rocksdb.isOwningHandle(),
                     "It seems RocksDB has been closed");
    }

    private RocksDB rocksdb() {
        this.checkValid();
        return this.rocksdb;
    }

    private ColumnFamilyHandle cf(String cf) {
        ColumnFamilyHandle cfh = this.cfs.get(cf);
        if (cfh == null) {
            throw new BackendException("Table '%s' is not opened", cf);
        }
        return cfh;
    }

    private Set<String> mergeOldCFs(String path, List<String> cfNames)
                                    throws RocksDBException {
        Set<String> cfs = listCFs(path);
        cfs.addAll(cfNames);
        return cfs;
    }

    private void ingestExternalFile() throws RocksDBException {
        String directory = this.config().get(RocksDBOptions.SST_PATH);
        if (directory == null || directory.isEmpty()) {
            return;
        }
        RocksDBIngester ingester = new RocksDBIngester(this.rocksdb);
        // Ingest all *.sst files in `directory`
        for (String cf : this.cfs.keySet()) {
            Path path = Paths.get(directory, cf);
            if (path.toFile().isDirectory()) {
                ingester.ingest(path, this.cf(cf));
            }
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

    @SuppressWarnings("deprecation") // setMaxBackgroundFlushes
    public static void initOptions(HugeConfig conf,
                                   DBOptionsInterface<?> db,
                                   ColumnFamilyOptionsInterface<?> cf,
                                   MutableColumnFamilyOptionsInterface<?> mcf) {
        final boolean optimize = conf.get(RocksDBOptions.OPTIMIZE_MODE);

        if (db != null) {
            db.setCreateIfMissing(true);

            // Optimize RocksDB
            if (optimize) {
                int processors = Runtime.getRuntime().availableProcessors();
                db.setIncreaseParallelism(Math.max(processors / 2, 1));

                db.setAllowConcurrentMemtableWrite(true);
                db.setEnableWriteThreadAdaptiveYield(true);
            }

            db.setInfoLogLevel(InfoLogLevel.valueOf(
                    conf.get(RocksDBOptions.LOG_LEVEL) + "_LEVEL"));

            /*
             * TODO: migrate to max_background_jobs option
             * https://github.com/facebook/rocksdb/pull/2205/files
             */
            db.setMaxBackgroundCompactions(
                    conf.get(RocksDBOptions.MAX_BG_COMPACTIONS));
            db.setMaxSubcompactions(
                    conf.get(RocksDBOptions.MAX_SUB_COMPACTIONS));
            db.setMaxBackgroundFlushes(
                    conf.get(RocksDBOptions.MAX_BG_FLUSHES));

            db.setDelayedWriteRate(conf.get(RocksDBOptions.DELAYED_WRITE_RATE));

            db.setAllowMmapWrites(
                    conf.get(RocksDBOptions.ALLOW_MMAP_WRITES));
            db.setAllowMmapReads(
                    conf.get(RocksDBOptions.ALLOW_MMAP_READS));

            db.setUseDirectReads(
                    conf.get(RocksDBOptions.USE_DIRECT_READS));
            db.setUseDirectIoForFlushAndCompaction(
                    conf.get(RocksDBOptions.USE_DIRECT_READS_WRITES_FC));

            db.setMaxOpenFiles(conf.get(RocksDBOptions.MAX_OPEN_FILES));
        }

        if (cf != null) {
            // Optimize RocksDB
            if (optimize) {
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

            // https://github.com/facebook/rocksdb/wiki/Block-Cache
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            long cacheCapacity = conf.get(RocksDBOptions.BLOCK_CACHE_CAPACITY);
            if (cacheCapacity <= 0L) {
                // Bypassing bug https://github.com/facebook/rocksdb/pull/5465
                tableConfig.setNoBlockCache(true);
            } else {
                tableConfig.setBlockCacheSize(cacheCapacity);
            }
            tableConfig.setPinL0FilterAndIndexBlocksInCache(
                    conf.get(RocksDBOptions.PIN_L0_FILTER_AND_INDEX_IN_CACHE));
            tableConfig.setCacheIndexAndFilterBlocks(
                    conf.get(RocksDBOptions.PUT_FILTER_AND_INDEX_IN_CACHE));

            // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
            int bitsPerKey = conf.get(RocksDBOptions.BLOOM_FILTER_BITS_PER_KEY);
            if (bitsPerKey >= 0) {
                boolean blockBased = conf.get(RocksDBOptions.BLOOM_FILTER_MODE);
                tableConfig.setFilter(new BloomFilter(bitsPerKey, blockBased));
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

    /**
     * StdSession implement for RocksDB
     */
    private final class StdSession extends RocksDBSessions.Session {

        private boolean closed;

        private WriteBatch batch;
        private WriteOptions writeOptions;

        public StdSession(HugeConfig conf) {
            this.closed = false;

            boolean bulkload = conf.get(RocksDBOptions.BULKLOAD_MODE);
            this.batch = new WriteBatch();
            this.writeOptions = new WriteOptions();
            this.writeOptions.setDisableWAL(bulkload);
            //this.writeOptions.setSync(false);
        }

        @Override
        public void close() {
            assert this.closeable();
            this.closed = true;
        }

        @Override
        public boolean closed() {
            return this.closed;
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.batch.count() > 0;
        }

        /**
         * Get property value by name from specified table
         */
        @Override
        public String property(String table, String property) {
            try {
                return rocksdb().getProperty(cf(table), property);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
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
            try {
                this.batch.put(cf(table), key, value);
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
            try {
                this.batch.merge(cf(table), key, value);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Merge a record to an existing key to a table and commit immediately
         */
        @Override
        public void increase(String table, byte[] key, byte[] value) {
            try {
                rocksdb().merge(cf(table), key, value);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Delete a record by key from a table
         */
        @Override
        public void remove(String table, byte[] key) {
            try {
                this.batch.singleDelete(cf(table), key);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Delete a record by key(or prefix with key) from a table
         */
        @Override
        public void delete(String table, byte[] key) {
            byte[] keyFrom = key;
            byte[] keyTo = Arrays.copyOf(key, key.length);
            keyTo = BinarySerializer.increaseOne(keyTo);
            try {
                this.batch.deleteRange(cf(table), keyFrom, keyTo);
            } catch (RocksDBException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void delete(String table, byte[] keyFrom, byte[] keyTo) {
            try {
                this.batch.deleteRange(cf(table), keyFrom, keyTo);
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

            try {
                return rocksdb().get(cf(table), key);
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
            RocksIterator iter = rocksdb().newIterator(cf(table));
            return new ColumnIterator(table, iter, null, null, SCAN_ANY);
        }

        /**
         * Scan records by key prefix from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            ReadOptions options = new ReadOptions();
            // NOTE: Options.prefix_extractor is a prerequisite
            options.setPrefixSameAsStart(true);
            RocksIterator iter = rocksdb().newIterator(cf(table), options);
            return new ColumnIterator(table, iter, prefix, null,
                                      SCAN_PREFIX_BEGIN);
        }

        /**
         * Scan records by key range from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            assert !this.hasChanges();
            ReadOptions options = new ReadOptions();
            options.setTotalOrderSeek(true); // Not sure if it must be set
            RocksIterator iter = rocksdb().newIterator(cf(table), options);
            return new ColumnIterator(table, iter, keyFrom, keyTo, scanType);
        }
    }

    /**
     * A wrapper for RocksIterator that convert RocksDB results to std Iterator
     */
    private static class ColumnIterator implements BackendColumnIterator {

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
                    // Just compare the prefix, maybe there are excess tail
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
