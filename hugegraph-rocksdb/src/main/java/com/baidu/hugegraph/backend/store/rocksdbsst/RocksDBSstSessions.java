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

package com.baidu.hugegraph.backend.store.rocksdbsst;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBStdSessions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.util.E;

public class RocksDBSstSessions extends RocksDBSessions {

    private final HugeConfig conf;
    private final String dataPath;
    private final Map<String, SstFileWriter> tables;

    public RocksDBSstSessions(HugeConfig conf, String dataPath,
                              String database, String store) {
        super(database, store);

        this.conf = conf;
        this.dataPath = dataPath;
        this.tables = new ConcurrentHashMap<>();

        File path = new File(this.dataPath);
        if (!path.exists()) {
            E.checkState(path.mkdirs(), "Can't mkdir '%s'", path);
        }
    }

    public RocksDBSstSessions(HugeConfig config, String dataPath,
                              String database, String store,
                              List<String> tableNames) throws RocksDBException {
        this(config, dataPath, database, store);
        for (String table : tableNames) {
            this.createTable(table);
        }
    }

    @Override
    public void open(HugeConfig config) throws Exception {
        // pass
    }

    @Override
    protected boolean opened() {
        return true;
    }

    @Override
    public Set<String> openedTables() {
        return this.tables.keySet();
    }

    @Override
    public void createTable(String table) throws RocksDBException {
        EnvOptions env = new EnvOptions();
        Options options = new Options();
        RocksDBStdSessions.initOptions(this.conf, options, options, options);
        // NOTE: unset merge op due to SIGSEGV when cf.setMergeOperatorName()
        options.setMergeOperatorName("not-exist-merge-op");
        SstFileWriter sst = new SstFileWriter(env, options);
        Path path = Paths.get(this.dataPath, table);
        sst.open(path.toString() + ".sst");
        this.tables.put(table, sst);
    }

    @Override
    public void dropTable(String table) throws RocksDBException {
        this.tables.remove(table);
    }

    private SstFileWriter table(String table) {
        SstFileWriter sst = this.tables.get(table);
        if (sst == null) {
            throw new BackendException("Table '%s' is not opened", table);
        }
        return sst;
    }

    @Override
    public final synchronized Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final synchronized Session newSession() {
        return new SstSession();
    }

    @Override
    protected synchronized void doClose() {
        final String NO_ENTRIES = "Cannot create sst file with no entries";

        for (SstFileWriter sst : this.tables.values()) {
            E.checkState(sst.isOwningHandle(), "SstFileWriter closed");
            try {
                sst.finish();
            } catch (RocksDBException e) {
                if (e.getMessage().equals(NO_ENTRIES)) {
                    continue;
                }
                throw new BackendException("Failed to close SstFileWriter", e);
            }
            sst.close();
        }
        this.tables.clear();
    }

    /**
     * SstSession implement for RocksDB
     */
    private final class SstSession extends Session {

        private boolean closed;
        private Map<String, Changes> batch;

        public SstSession() {
            this.closed = false;
            this.batch = new HashMap<>();
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
            return this.batch.size() > 0;
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {
            int count = this.batch.size();
            if (count <= 0) {
                return 0;
            }

            try {
                for (Entry<String, Changes> table : this.batch.entrySet()) {
                    if (table.getValue().isEmpty() ||
                        table.getKey().endsWith("i")) {
                        // Skip empty value table or index table
                        continue;
                    }

                    // TODO: limit individual SST file size
                    SstFileWriter sst = table(table.getKey());
                    for (Pair<byte[], byte[]> change : table.getValue()) {
                        sst.put(change.getKey(), change.getValue());
                    }
                }
            } catch (RocksDBException e) {
                throw new BackendException("Failed to commit", e);
            }

            // Clear batch if write() successfully (retained if failed)
            this.batch.clear();

            return count;
        }

        /**
         * Get property value
         */
        @Override
        public String property(String property) {
            throw new NotSupportException("RocksDBSstStore property()");
        }

        /**
         * Get property value by name from specified table
         */
        @Override
        public String property(String table, String property) {
            throw new NotSupportException("RocksDBSstStore property()");
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] key, byte[] value) {
            Changes changes = this.batch.get(table);
            if (changes == null) {
                changes = new Changes();
                this.batch.put(table, changes);
            }
            changes.add(Pair.of(key, value));
        }

        /**
         * Merge a record to an existing key to a table
         * For more details about merge-operator:
         *  https://github.com/facebook/rocksdb/wiki/merge-operator
         */
        @Override
        public void merge(String table, byte[] key, byte[] value) {
            throw new NotSupportException("RocksDBSstStore merge()");
        }

        /**
         * Merge a record to an existing key to a table and commit immediately
         */
        @Override
        public void increase(String table, byte[] key, byte[] value) {
            throw new NotSupportException("RocksDBSstStore increase()");
        }

        /**
         * Delete a record by key from a table
         */
        @Override
        public void remove(String table, byte[] key) {
            throw new NotSupportException("RocksDBSstStore remove()");
        }

        /**
         * Delete a record by key(or prefix with key) from a table
         */
        @Override
        public void delete(String table, byte[] key) {
            throw new NotSupportException("RocksDBSstStore delete()");
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void delete(String table, byte[] keyFrom, byte[] keyTo) {
            throw new NotSupportException("RocksDBSstStore delete()");
        }

        /**
         * Get a record by key from a table
         */
        @Override
        public byte[] get(String table, byte[] key) {
            return null;
        }

        /**
         * Scan all records from a table
         */
        @Override
        public BackendColumnIterator scan(String table) {
            assert !this.hasChanges();
            return BackendColumnIterator.empty();
        }

        /**
         * Scan records by key prefix from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            return BackendColumnIterator.empty();
        }

        /**
         * Scan records by key range from a table
         */
        @Override
        public BackendColumnIterator scan(String table,
                                          byte[] keyFrom,
                                          byte[] keyTo,
                                          int scanType) {
            assert !this.hasChanges();
            return BackendColumnIterator.empty();
        }
    }

    private static class Changes extends ArrayList<Pair<byte[], byte[]>> {

        private static final long serialVersionUID = 9047034706183029125L;
    }
}
