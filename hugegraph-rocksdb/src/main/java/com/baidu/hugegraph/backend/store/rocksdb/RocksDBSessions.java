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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.primitives.UnsignedBytes;

public class RocksDBSessions extends BackendSessionPool {

    private final RocksDB rocksdb;
    private final Map<String, ColumnFamilyHandle> cfs;

    public RocksDBSessions(String path, String walPath)
                           throws RocksDBException {
        this.cfs = new HashMap<>();

        // Don't merge old CFs, we expect a clear DB when using this one
        Options options = new Options();
        this.initOptions(options, options);
        options.setWalDir(walPath);
        this.rocksdb = RocksDB.open(options, path);
    }

    public RocksDBSessions(String path, String walPath, List<String> cfNames)
                           throws RocksDBException {
        this.cfs = new HashMap<>();

        // Old CFs should always be opened
        List<String> cfs = this.mergeOldCFs(path, cfNames);

        List<ColumnFamilyDescriptor> cfds = new ArrayList<>(cfs.size());
        for (String cf : cfs) {
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encode(cf));
            this.initOptions(null, cfd.columnFamilyOptions());
            cfds.add(cfd);
        }

        List<ColumnFamilyHandle> cfhs = new ArrayList<>();
        DBOptions options = new DBOptions();
        this.initOptions(options, null);
        options.setWalDir(walPath);
        this.rocksdb = RocksDB.open(options, path, cfds, cfhs);

        E.checkState(cfhs.size() == cfs.size(),
                     "Excepct same size of cf-handles and cf-names");
        for (int i = 0; i < cfs.size(); i++) {
            this.cfs.put(cfs.get(i), cfhs.get(i));
        }
    }

    public Set<String> openedTables() {
        return this.cfs.keySet();
    }

    public void createTable(String table) throws RocksDBException {
        if (this.cfs.containsKey(table)) {
            return;
        }
        // Should we use options.setCreateMissingColumnFamilies() to create CF
        ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encode(table));
        this.initOptions(null, cfd.columnFamilyOptions());
        this.cfs.put(table, this.rocksdb.createColumnFamily(cfd));
    }

    public void dropTable(String table) throws RocksDBException {
        ColumnFamilyHandle cfh = cf(table);
        this.rocksdb.dropColumnFamily(cfh);
        cfh.close();
        this.cfs.remove(table);
    }

    public final synchronized Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final synchronized Session newSession() {
        E.checkState(this.rocksdb != null,
                     "RocksDB has not been initialized");
        return new Session();
    }

    @Override
    protected synchronized void doClose() {
        for (ColumnFamilyHandle cf : this.cfs.values()) {
            cf.close();
        }
        this.cfs.clear();

        this.rocksdb.close();
    }

    private RocksDB rocksdb() {
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

    private void initOptions(DBOptionsInterface<?> db,
                             ColumnFamilyOptionsInterface<?> cf) {
        if (db != null) {
            db.setCreateIfMissing(true);

            // Optimize RocksDB
            final int processors = Runtime.getRuntime().availableProcessors();
            db.setIncreaseParallelism(processors);
        }

        if (cf != null) {
            // Optimize RocksDB
            cf.optimizeLevelStyleCompaction();
            cf.optimizeUniversalStyleCompaction();

            // https://github.com/facebook/rocksdb/tree/master/utilities/merge_operators
            cf.setMergeOperatorName("uint64add"); // uint64add/stringappend
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
     * Session for RocksDB
     */
    public final class Session extends BackendSessionPool.Session {

        private boolean closed;

        private WriteBatch batch;
        private WriteOptions writeOptions;

        public Session() {
            this.closed = false;

            this.batch = new WriteBatch();
            this.writeOptions = new WriteOptions();
            //this.writeOptions.setDisableWAL(true);
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
        public void put(String table, byte[] key, byte[] value) {
            this.batch.put(cf(table), key, value);
        }

        /**
         * Merge a record to an existing key to a table
         * For more details about merge-operator:
         *  https://github.com/facebook/rocksdb/wiki/merge-operator
         */
        public void merge(String table, byte[] key, byte[] value) {
            this.batch.merge(cf(table), key, value);
        }

        /**
         * Delete a record by key from a table
         */
        public void remove(String table, byte[] key) {
            this.batch.remove(cf(table), key);
        }

        /**
         * Delete a record by key(or prefix with key) from a table
         */
        public void delete(String table, byte[] key) {
            byte[] keyFrom = key;
            byte[] keyTo = Arrays.copyOf(key, key.length);
            keyTo = increase(keyTo);
            this.batch.deleteRange(cf(table), keyFrom, keyTo);
        }

        /**
         * Delete a range of keys from a table
         */
        public void delete(String table, byte[] keyFrom, byte[] keyTo) {
            this.batch.deleteRange(cf(table), keyFrom, keyTo);
        }

        /**
         * Get a record by key from a table
         */
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
        public Iterator<BackendColumn> scan(String table) {
            assert !this.hasChanges();
            RocksIterator itor = rocksdb().newIterator(cf(table));
            return new ColumnIterator(table, itor, null, null, false);
        }

        /**
         * Scan records by key prefix from a table
         */
        public Iterator<BackendColumn> scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            // NOTE: Options.prefix_extractor is a prerequisite
            //ReadOptions.setPrefixSameAsStart(true);
            RocksIterator itor = rocksdb().newIterator(cf(table));
            return new ColumnIterator(table, itor, prefix, null, true);
        }

        /**
         * Scan records by key range from a table
         */
        public Iterator<BackendColumn> scan(String table,
                                            byte[] keyFrom,
                                            byte[] keyTo) {
            assert !this.hasChanges();
            ReadOptions options = new ReadOptions();
            options.setTotalOrderSeek(true); // Not sure if it must be set
            RocksIterator itor = rocksdb().newIterator(cf(table), options);
            return new ColumnIterator(table, itor, keyFrom, keyTo, false);
        }
    }

    /**
     * A wrapper for RocksIterator that convert RocksDB results to std Iterator
     */
    private static class ColumnIterator implements Iterator<BackendColumn> {

        private final String table;
        private final RocksIterator itor;
        private byte[] keyBegin;
        private byte[] keyEnd;
        private boolean matchPrefix;

        // Don't use BytewiseComparator
        private static final Comparator<byte[]> C =
                             UnsignedBytes.lexicographicalComparator();

        public ColumnIterator(String table,
                              RocksIterator itor,
                              byte[] keyBegin,
                              byte[] keyEnd,
                              boolean matchPrefix) {
            E.checkNotNull(itor, "itor");
            this.table = table;

            this.itor = itor;
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.matchPrefix = matchPrefix;

            if (matchPrefix && keyEnd != null) {
                throw new IllegalArgumentException(
                          "Param keyEnd must be null when matchPrefix=true");
            }

            //this.dump();

            if (keyBegin != null) {
                this.itor.seek(keyBegin);
            } else {
                this.itor.seekToFirst();
            }
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
            boolean matched = false;
            if (this.itor.isOwningHandle() && this.itor.isValid()) {
                if (this.matchPrefix) {
                    // Prefix match? TODO: use custom prefix_extractor instead
                    matched = Bytes.prefixWith(this.itor.key(), this.keyBegin);
                } else if (this.keyEnd != null) {
                    // Range match?
                    matched = C.compare(this.itor.key(), this.keyEnd) < 0;
                } else {
                    // Any match
                    matched = true;
                }
            }
            if (!matched) {
                // Free if finished
                this.itor.close();
            }
            return matched;
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
            return entry;
        }
    }
}
