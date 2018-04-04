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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

public class RocksDBStdSessions extends RocksDBSessions {

    private final HugeConfig conf;
    private final RocksDB rocksdb;
    private final Map<String, ColumnFamilyHandle> cfs;

    public RocksDBStdSessions(HugeConfig conf, String dataPath, String walPath)
                              throws RocksDBException {
        this.conf = conf;
        this.cfs = new HashMap<>();

        // Init options
        Options options = new Options();
        initOptions(this.conf, options, options, options);
        options.setWalDir(walPath);

        /*
         * Open RocksDB at the first time
         * Don't merge old CFs, we expect a clear DB when using this one
         */
        this.rocksdb = RocksDB.open(options, dataPath);
    }

    public RocksDBStdSessions(HugeConfig conf, String dataPath, String walPath,
                              List<String> cfNames) throws RocksDBException {
        this.conf = conf;
        this.cfs = new HashMap<>();

        // Old CFs should always be opened
        List<String> cfs = this.mergeOldCFs(dataPath, cfNames);

        // Init CFs options
        List<ColumnFamilyDescriptor> cfds = new ArrayList<>(cfs.size());
        for (String cf : cfs) {
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encode(cf));
            ColumnFamilyOptions options = cfd.columnFamilyOptions();
            initOptions(this.conf, null, options, options);
            cfds.add(cfd);
        }

        // Init DB options
        DBOptions options = new DBOptions();
        initOptions(this.conf, options, null, null);
        options.setWalDir(walPath);

        // Open RocksDB with CFs
        List<ColumnFamilyHandle> cfhs = new ArrayList<>();
        this.rocksdb = RocksDB.open(options, dataPath, cfds, cfhs);
        E.checkState(cfhs.size() == cfs.size(),
                     "Excepct same size of cf-handles and cf-names");

        // Collect CF Handles
        for (int i = 0; i < cfs.size(); i++) {
            this.cfs.put(cfs.get(i), cfhs.get(i));
        }

        ingestExternalFile();
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
        ColumnFamilyOptions options = cfd.columnFamilyOptions();
        initOptions(this.conf, null, options, options);
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
    public final synchronized Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final synchronized Session newSession() {
        E.checkState(this.rocksdb != null,
                     "RocksDB has not been initialized");
        return new StdSession(this.conf);
    }

    @Override
    protected synchronized void doClose() {
        this.checkValid();

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
        assert this.rocksdb.isOwningHandle();
        return this.rocksdb;
    }

    private ColumnFamilyHandle cf(String cf) {
        ColumnFamilyHandle cfh = this.cfs.get(cf);
        if (cfh == null) {
            throw new BackendException("Table '%s' is not opened", cf);
        }
        return cfh;
    }

    private List<String> mergeOldCFs(String path, List<String> cfNames)
                                     throws RocksDBException {
        List<String> cfs = new ArrayList<>(cfNames);

        List<byte[]> oldCFs = RocksDB.listColumnFamilies(new Options(), path);
        if (oldCFs.isEmpty()) {
            cfs.add("default");
        } else {
            for (byte[] oldCF : oldCFs) {
                String old = decode(oldCF);
                if (!cfNames.contains(old)) {
                    cfs.add(old);
                }
            }
        }
        return cfs;
    }

    private void ingestExternalFile() throws RocksDBException {
        String directory = this.conf.get(RocksDBOptions.SST_PATH);
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

            cf.setNumLevels(conf.get(RocksDBOptions.NUM_LEVELS));
            cf.setCompactionStyle(CompactionStyle.valueOf(
                    conf.get(RocksDBOptions.COMPACTION_STYLE)));

            cf.setMinWriteBufferNumberToMerge(
                    conf.get(RocksDBOptions.MIN_MEMTABLES_TO_MERGE));
            cf.setMaxWriteBufferNumberToMaintain(
                    conf.get(RocksDBOptions.MAX_MEMTABLES_TO_MAINTAIN));

            // https://github.com/facebook/rocksdb/tree/master/utilities/merge_operators
            cf.setMergeOperatorName("uint64add"); // uint64add/stringappend
        }

        if (mcf != null) {
            mcf.setCompressionType(CompressionType.getCompressionType(
                    conf.get(RocksDBOptions.COMPRESSION_TYPE)));

            mcf.setWriteBufferSize(
                    conf.get(RocksDBOptions.MEMTABLE_SIZE));
            mcf.setMaxWriteBufferNumber(
                    conf.get(RocksDBOptions.MAX_MEMTABLES));

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

    public static final byte[] increase(byte[] bytes) {
        final byte BYTE_MAX_VALUE = (byte) 0xff;
        assert bytes.length > 0;
        byte last = bytes[bytes.length - 1];
        if (last != BYTE_MAX_VALUE) {
            bytes[bytes.length - 1] += 0x01;
        } else {
            // Process overflow (like [1, 255] => [2, 0])
            int i = bytes.length - 1;
            for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
                bytes[i] += 0x01;
            }
            if (bytes[i] == BYTE_MAX_VALUE) {
                assert i == 0;
                throw new BackendException("Unable to increase bytes: %s",
                                           Bytes.toHex(bytes));
            }
            bytes[i] += 0x01;
        }
        return bytes;
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
         * Clear updates not committed in the session
         */
        @Override
        public void clear() {
            this.batch.clear();
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
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] key, byte[] value) {
            this.batch.put(cf(table), key, value);
        }

        /**
         * Merge a record to an existing key to a table
         * For more details about merge-operator:
         *  https://github.com/facebook/rocksdb/wiki/merge-operator
         */
        @Override
        public void merge(String table, byte[] key, byte[] value) {
            this.batch.merge(cf(table), key, value);
        }

        /**
         * Delete a record by key from a table
         */
        @Override
        public void remove(String table, byte[] key) {
            this.batch.remove(cf(table), key);
        }

        /**
         * Delete a record by key(or prefix with key) from a table
         */
        @Override
        public void delete(String table, byte[] key) {
            byte[] keyFrom = key;
            byte[] keyTo = Arrays.copyOf(key, key.length);
            keyTo = increase(keyTo);
            this.batch.deleteRange(cf(table), keyFrom, keyTo);
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void delete(String table, byte[] keyFrom, byte[] keyTo) {
            this.batch.deleteRange(cf(table), keyFrom, keyTo);
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
            RocksIterator itor = rocksdb().newIterator(cf(table));
            return new ColumnIterator(table, itor, null, null, SCAN_ANY);
        }

        /**
         * Scan records by key prefix from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            // NOTE: Options.prefix_extractor is a prerequisite
            //ReadOptions.setPrefixSameAsStart(true);
            RocksIterator itor = rocksdb().newIterator(cf(table));
            return new ColumnIterator(table, itor, prefix, null,
                                      SCAN_PREFIX_WITH_BEGIN);
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
            RocksIterator itor = rocksdb().newIterator(cf(table), options);
            return new ColumnIterator(table, itor, keyFrom, keyTo, scanType);
        }
    }

    /**
     * A wrapper for RocksIterator that convert RocksDB results to std Iterator
     */
    private static class ColumnIterator implements BackendColumnIterator {

        private final String table;
        private final RocksIterator itor;
        private final byte[] keyBegin;
        private final byte[] keyEnd;
        private final int scanType;

        private byte[] position;

        public ColumnIterator(String table, RocksIterator itor,
                              byte[] keyBegin, byte[] keyEnd, int scanType) {
            E.checkNotNull(itor, "itor");
            this.table = table;

            this.itor = itor;
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.scanType = scanType;

            this.position = keyBegin;

            this.checkArguments();

            //this.dump();

            this.seek();
        }

        private void checkArguments() {
            E.checkArgument(!(this.match(Session.SCAN_PREFIX_WITH_BEGIN) &&
                              this.match(Session.SCAN_PREFIX_WITH_END)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_PREFIX_WITH_END at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_WITH_BEGIN) &&
                              this.match(Session.SCAN_GT_BEGIN)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_GT_BEGIN/SCAN_GTE_BEGIN at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_WITH_END) &&
                              this.match(Session.SCAN_LT_END)),
                            "Can't set SCAN_PREFIX_WITH_END and " +
                            "SCAN_LT_END/SCAN_LTE_END at the same time");

            if (this.match(Session.SCAN_PREFIX_WITH_BEGIN)) {
                E.checkArgument(this.keyBegin != null,
                                "Parameter `keyBegin` can't be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
                E.checkArgument(this.keyEnd == null,
                                "Parameter `keyEnd` must be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
            }

            if (this.match(Session.SCAN_PREFIX_WITH_END)) {
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
            return (expected & this.scanType) == expected;
        }

        /**
         * Just for debug
         */
        @SuppressWarnings("unused")
        private void dump() {
            this.itor.seekToFirst();
            System.out.println(">>>> seek from " + this.table + ": "  +
                               (this.keyBegin == null ? "*" :
                                StringEncoding.decode(this.keyBegin)) +
                               (this.itor.isValid() ? "" : " - No data"));
            for (; this.itor.isValid(); this.itor.next()) {
                System.out.println(StringEncoding.decode(this.itor.key()) +
                                   ": " +
                                   StringEncoding.decode(this.itor.value()));
            }
        }

        @Override
        public boolean hasNext() {
            boolean matched = this.itor.isOwningHandle() && this.itor.isValid();
            if (matched && !this.match(Session.SCAN_ANY)) {
                matched = this.filter(this.itor.key());
            }
            if (!matched) {
                // Free the iterator if finished
                this.itor.close();
            }
            return matched;
        }

        private void seek() {
            if (this.keyBegin == null) {
                // Seek to the first if no `keyBegin`
                this.itor.seekToFirst();
            } else {
                /*
                 * Seek to `keyBegin`:
                 * if set SCAN_GT_BEGIN/SCAN_GTE_BEGIN (key > / >= 'xx')
                 * or if set SCAN_PREFIX_WITH_BEGIN (key prefix with 'xx')
                 */
                this.itor.seek(this.keyBegin);

                // Skip `keyBegin` if set SCAN_GT_BEGIN (key > 'xx')
                if (this.match(Session.SCAN_GT_BEGIN) &&
                    !this.match(Session.SCAN_GTE_BEGIN)) {
                    while (this.hasNext() && !Bytes.equals(this.itor.key(),
                                                           this.keyBegin)) {
                        this.next();
                    }
                }
            }
        }

        private boolean filter(byte[] key) {
            if (this.match(Session.SCAN_PREFIX_WITH_BEGIN)) {
                /*
                 * Prefix with `keyBegin`?
                 * TODO: use custom prefix_extractor instead
                 */
                return Bytes.prefixWith(key, this.keyBegin);
            } else if (this.match(Session.SCAN_PREFIX_WITH_END)) {
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
                    // Just compare the prefix，maybe there are excess tail
                    key = Arrays.copyOfRange(key, 0, this.keyEnd.length);
                    return Bytes.compare(key, this.keyEnd) <= 0;
                } else {
                    return Bytes.compare(key, this.keyEnd) < 0;
                }
            } else {
                assert this.match(Session.SCAN_ANY) : "Unknow scan type";
                return true;
            }
        }

        @Override
        public BackendColumn next() {
            if (!this.itor.isOwningHandle() || !this.itor.isValid()) {
                throw new NoSuchElementException();
            }

            BackendColumn entry = new BackendColumn();
            entry.name = this.itor.key();
            entry.value = this.itor.value();

            this.itor.next();

            if (this.itor.isValid()) {
                this.position = entry.name;
            } else {
                // The end
                this.position = null;
            }

            return entry;
        }

        @Override
        public byte[] position() {
            return this.position;
        }

        @Override
        public void close() {
            if (this.itor.isOwningHandle()) {
                this.itor.close();
            }
        }
    }
}
