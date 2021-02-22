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

package com.baidu.hugegraph.backend.store.tikv;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

public class TikvStdSessions extends TikvSessions {

    private final HugeConfig config;

    private volatile RawKVClient tikvClient;

    private final Map<String, Integer> tables;
    private final AtomicInteger refCount;

    private static int tableCode = 0;

    public TikvStdSessions(HugeConfig config, String database, String store) {
        super(config, database, store);
        this.config = config;

        TiConfiguration conf = TiConfiguration.createRawDefault(
                               this.config.get(TikvOptions.TIKV_PDS));
        conf.setBatchGetConcurrency(
                this.config.get(TikvOptions.TIKV_BATCH_GET_CONCURRENCY));
        conf.setBatchPutConcurrency(
                this.config.get(TikvOptions.TIKV_BATCH_PUT_CONCURRENCY));
        conf.setBatchDeleteConcurrency(
                this.config.get(TikvOptions.TIKV_BATCH_DELETE_CONCURRENCY));
        conf.setBatchScanConcurrency(
                this.config.get(TikvOptions.TIKV_BATCH_SCAN_CONCURRENCY));
        conf.setDeleteRangeConcurrency(
                this.config.get(TikvOptions.TIKV_DELETE_RANGE_CONCURRENCY));
        TiSession session = TiSession.create(conf);
        this.tikvClient = session.createRawClient();

        this.tables = new ConcurrentHashMap<>();
        this.refCount = new AtomicInteger(1);
    }

    @Override
    public void open() throws Exception {
        // pass
    }

    @Override
    protected boolean opened() {
        return this.tikvClient != null;
    }

    @Override
    public Set<String> openedTables() {
        return this.tables.keySet();
    }

    @Override
    public synchronized void createTable(String... tables) {
        for (String table : tables) {
            this.tables.put(table, tableCode++);
        }
    }

    @Override
    public synchronized void dropTable(String... tables) {
        for (String table : tables) {
            this.tables.remove(table);
        }
    }

    @Override
    public boolean existsTable(String table) {
        return this.tables.containsKey(table);
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final Session newSession() {
        return new StdSession(this.config());
    }

    @Override
    protected synchronized void doClose() {
        this.checkValid();

        if (this.refCount.decrementAndGet() > 0) {
            return;
        }
        assert this.refCount.get() == 0;

        this.tables.clear();

        this.tikvClient.close();
    }

    private void checkValid() {
    }

    private RawKVClient tikv() {
        this.checkValid();
        return this.tikvClient;
    }

    public static final byte[] encode(String string) {
        return StringEncoding.encode(string);
    }

    public static final String decode(byte[] bytes) {
        return StringEncoding.decode(bytes);
    }

    /**
     * StdSession implement for tikv
     */
    private final class StdSession extends Session {

        private Map<ByteString, ByteString> putBatch;
        private List<ByteString> deleteBatch;
        private Set<ByteString> deletePrefixBatch;
        private Map<ByteString, ByteString> deleteRangeBatch;

        public StdSession(HugeConfig conf) {
            this.putBatch = new HashMap<>();
            this.deleteBatch = new ArrayList<>();
            this.deletePrefixBatch = new HashSet<>();
            this.deleteRangeBatch = new HashMap<>();
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
            return !this.opened;
        }

        @Override
        public void reset() {
            this.putBatch = new HashMap<>();
            this.deleteBatch = new ArrayList<>();
            this.deletePrefixBatch = new HashSet<>();
            this.deleteRangeBatch = new HashMap<>();
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.size() > 0;
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {

            int count = this.size();
            if (count <= 0) {
                return 0;
            }

            if (this.putBatch.size() > 0) {
                tikv().batchPutAtomic(this.putBatch);
                this.putBatch.clear();
            }

            if (this.deleteBatch.size() > 0) {
                tikv().batchDeleteAtomic(this.deleteBatch);
                this.deleteBatch.clear();
            }

            if (this.deletePrefixBatch.size() > 0) {
                for (ByteString key : this.deletePrefixBatch) {
                    tikv().deletePrefix(key);
                }
            }

            if (this.deleteRangeBatch.size() > 0) {
                for (Map.Entry<ByteString, ByteString> entry :
                     this.deleteRangeBatch.entrySet()) {
                    tikv().deleteRange(entry.getKey(), entry.getValue());
                }
            }

            return count;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            this.putBatch.clear();
            this.deleteBatch.clear();
            this.deletePrefixBatch.clear();
            this.deleteRangeBatch.clear();
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            // TODO: get first key and lastkey of fake tikv table
            return null;
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] key, byte[] value) {
            this.putBatch.put(this.key(table, key), ByteString.copyFrom(value));
        }

        @Override
        public synchronized void increase(String table, byte[] key, byte[] value) {
            long old = 0L;
            byte[] oldValue = this.get(table, key);
            if (oldValue.length != 0) {
                old = this.l(oldValue);
            }
            ByteString newValue = ByteString.copyFrom(
                                  this.b(old + this.l(value)));
            tikv().put(this.key(table, key), newValue);
        }

        @Override
        public void delete(String table, byte[] key) {
            this.deleteBatch.add(this.key(table, key));
        }

        @Override
        public void deletePrefix(String table, byte[] key) {
            ByteString deleteKey = this.key(table, key);
            this.deletePrefixBatch.add(deleteKey);
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void deleteRange(String table, byte[] keyFrom, byte[] keyTo) {
            ByteString startKey = this.key(table, keyFrom);
            ByteString endKey = this.key(table, keyTo);
            this.deleteRangeBatch.put(startKey, endKey);
        }

        @Override
        public byte[] get(String table, byte[] key) {
            return tikv().get(this.key(table, key)).toByteArray();
        }

        @Override
        public BackendColumnIterator scan(String table) {
            assert !this.hasChanges();
            Iterator<Kvrpcpb.KvPair> results = tikv().scanPrefix0(this.key(table));
            return new ColumnIterator(table, results);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            Iterator<Kvrpcpb.KvPair> results = tikv().scanPrefix0(
                                               this.key(table, prefix));
            return new ColumnIterator(table, results);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            assert !this.hasChanges();
            Iterator<Kvrpcpb.KvPair> results;
            if (keyFrom == null) {
                results = tikv().scanPrefix0(this.key(table));
            } else {
                if (keyTo == null) {
                    results = tikv().scan0(this.key(table, keyFrom), Key.toRawKey(this.key(table)).nextPrefix().toByteString());
                } else {
                    results = tikv().scan0(this.key(table, keyFrom), Key.toRawKey(this.key(table, keyTo)).nextPrefix().toByteString());
                }
            }
            return new ColumnIterator(table, results, keyFrom, keyTo, scanType);
        }

        private ByteString key(String table) {
            byte[] prefix = table.getBytes();
            byte[] actualKey = new byte[prefix.length + 1];
            System.arraycopy(prefix, 0, actualKey, 0, prefix.length);
            actualKey[prefix.length] = (byte) 0xff;
            return ByteString.copyFrom(actualKey);
        }

        private ByteString key(String table, byte[] key) {
            byte[] prefix = table.getBytes();
            byte[] actualKey = new byte[prefix.length + 1 + key.length];
            System.arraycopy(prefix, 0, actualKey, 0, prefix.length);
            actualKey[prefix.length] = (byte) 0xff;
            System.arraycopy(key, 0, actualKey, prefix.length + 1, key.length);
            return ByteString.copyFrom(actualKey);
        }

        private byte[] b(long value) {
            return ByteBuffer.allocate(Long.BYTES)
                             .order(ByteOrder.nativeOrder())
                             .putLong(value).array();
        }

        private long l(byte[] bytes) {
            assert bytes.length == Long.BYTES;
            return ByteBuffer.wrap(bytes)
                             .order(ByteOrder.nativeOrder())
                             .getLong();
        }

        private int size() {
            return this.putBatch.size() + this.deleteBatch.size() +
                   this.deletePrefixBatch.size() + this.deleteRangeBatch.size();
        }
    }

    private static class ColumnIterator implements BackendColumnIterator,
                                        Countable {

        private final String table;
        private final Iterator<Kvrpcpb.KvPair> iter;

        private final byte[] keyBegin;
        private final byte[] keyEnd;
        private final int scanType;

        private byte[] position;
        private byte[] value;
        private boolean matched;

        public ColumnIterator(String table, Iterator<Kvrpcpb.KvPair> results) {
            this(table, results, null, null, 0);
        }

        public ColumnIterator(String table, Iterator<Kvrpcpb.KvPair> results,
                              byte[] keyBegin, byte[] keyEnd, int scanType) {
            E.checkNotNull(results, "results");
            this.table = table;

            this.iter = results;
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.scanType = scanType;

            this.position = keyBegin;
            this.value = null;
            this.matched = false;

            this.checkArguments();
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

        @Override
        public boolean hasNext() {
            // Update position for paging
            if (!this.iter.hasNext()) {
                return false;
            }
            Kvrpcpb.KvPair next = this.iter.next();

            byte[] tikvKey = next.getKey().toByteArray();
            byte[] prefix = this.table.getBytes();
            int length = tikvKey.length - prefix.length - 1;
            byte[] key = new byte[length];
            System.arraycopy(tikvKey, prefix.length + 1, key, 0, length);
            this.position = key;
            this.value = next.getValue().toByteArray();

            // Do filter if not SCAN_ANY
            if (!this.match(Session.SCAN_ANY)) {
                this.matched = this.filter(this.position);
            }
            if (!this.matched) {
                // The end
                this.position = null;
                // Free the iterator if finished
                this.close();
            }
            return this.matched;
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
        public BackendEntry.BackendColumn next() {
            if (!this.matched) {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
            }
            BackendEntry.BackendColumn col = BackendEntry.BackendColumn.of(
                                             this.position, this.value);
            this.matched = false;

            return col;
        }

        @Override
        public long count() {
            long count = 0L;
            while (this.hasNext()) {
                this.next();
                count++;
                this.matched = false;
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
        }
    }
}
