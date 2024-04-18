/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.rocksdb.access;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.CFHandleLock;
import org.apache.hugegraph.rocksdb.access.util.Asserts;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.util.Bytes;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionOperatorImpl implements SessionOperator {

    private final RocksDB db;
    private final RocksDBSession session;
    private WriteBatch batch;

    public SessionOperatorImpl(RocksDBSession session) {
        this.session = session;
        this.db = session.getDB();
    }

    public static final byte[] increaseOne(byte[] bytes) {
        final byte BYTE_MAX_VALUE = (byte) 0xff;
        assert bytes.length > 0;
        byte last = bytes[bytes.length - 1];
        if (last != BYTE_MAX_VALUE) {
            bytes[bytes.length - 1] += (byte) 0x01;
        } else {
            // Process overflow (like [1, 255] => [2, 0])
            int i = bytes.length - 1;
            for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
                bytes[i] += (byte) 0x01;
            }
            if (bytes[i] == BYTE_MAX_VALUE) {
                assert i == 0;
                throw new DBStoreException("Unable to increase bytes: %s", Bytes.toHex(bytes));
            }
            bytes[i] += (byte) 0x01;
        }
        return bytes;
    }

    public RocksDB rocksdb() {
        return db;
    }

    @Override
    public RocksDBSession getDBSession() {
        return session;
    }

    private CFHandleLock getLock(String table) {
        CFHandleLock cf = this.session.getCFHandleLock(table);
        return cf;
    }

    @Override
    public HgPair<byte[], byte[]> keyRange(String table) {
        byte[] startKey, endKey;
        try (CFHandleLock handle = this.getLock(table);
             RocksIterator iter = rocksdb().newIterator(handle.get())) {
            iter.seekToFirst();
            if (!iter.isValid()) {
                return null;
            }
            startKey = iter.key();
            iter.seekToLast();
            if (!iter.isValid()) {
                return new HgPair<>(startKey, null);
            }
            endKey = iter.key();
        }
        return new HgPair<>(startKey, endKey);
    }

    @Override
    public void compactRange(String table) throws DBStoreException {
        try (CFHandleLock handle = this.getLock(table)) {
            rocksdb().compactRange(handle.get());
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    @Override
    public void compactRange() throws DBStoreException {
        for (String name : session.getTables().keySet()) {
            compactRange(name);
        }
    }

    @Override
    public void put(String table, byte[] key, byte[] value) throws DBStoreException {
        try {
            this.getBatch().put(session.getCF(table), key, value);
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    /*
     * only support 'long data' operator
     */
    @Override
    public void merge(String table, byte[] key, byte[] value) throws DBStoreException {
        try {
            this.getBatch().merge(session.getCF(table), key, value);
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    @Override
    public void increase(String table, byte[] key, byte[] value) throws DBStoreException {
        try (CFHandleLock cf = this.getLock(table)) {
            rocksdb().merge(cf.get(), key, value);
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    @Override
    public void delete(String table, byte[] key) throws DBStoreException {
        try {
            this.getBatch().delete(session.getCF(table), key);
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    @Override
    public void deleteSingle(String table, byte[] key) throws DBStoreException {
        try {
            this.getBatch().singleDelete(session.getCF(table), key);
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    @Override
    public void deletePrefix(String table, byte[] key) throws DBStoreException {
        byte[] keyFrom = key;
        byte[] keyTo = Arrays.copyOf(key, key.length);
        keyTo = increaseOne(keyTo);
        try {
            this.getBatch().deleteRange(session.getCF(table), keyFrom, keyTo);
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    @Override
    public void deleteRange(String table, byte[] keyFrom, byte[] keyTo) throws DBStoreException {
        Asserts.isTrue(keyFrom != null, "KeyFrom is null");
        Asserts.isTrue(keyTo != null, "KeyTo is null");

        if (Bytes.compare(keyTo, keyFrom) < 0) {
            throw new DBStoreException("[end key: %s ] is lower than [start key: %s]",
                                       Arrays.toString(keyTo), Arrays.toString(keyFrom));
        }

        try {
            this.prepare();
            this.getBatch().deleteRange(session.getCF(table), keyFrom, keyTo);
            this.commit();
        } catch (RocksDBException e) {
            this.rollback();
            throw new DBStoreException(e);
        }
    }

    @Override
    public void deleteRange(byte[] keyFrom, byte[] keyTo) throws DBStoreException {
        for (String name : session.getTables().keySet()) {
            deleteRange(name, keyFrom, keyTo);
        }
    }

    @Override
    public byte[] get(String table, byte[] key) throws DBStoreException {
        try (CFHandleLock cf = this.getLock(table)) {
            return rocksdb().get(cf.get(), key);
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    @Override
    public void prepare() {
        this.session.getCfHandleReadLock().lock();
    }

    /**
     * commit抛出异常后一定要调用rollback，否则会造成cfHandleReadLock未释放
     */
    @Override
    public Integer commit() throws DBStoreException {
        int count = this.getBatch().count();
        if (count > 0) {
            try {
                rocksdb().write(session.getWriteOptions(), this.batch);
            } catch (RocksDBException e) {
                throw new DBStoreException(e);
            }
            // Clear batch if write() successfully (retained if failed)
            this.batch.clear();
            this.batch.close();
            this.batch = null;
        }
        this.session.getCfHandleReadLock().unlock();
        return count;
    }

    @Override
    public void rollback() {
        try {
            if (this.batch != null) {
                this.batch.clear();
                this.batch.close();
            }
            this.batch = null;
        } finally {
            try {
                this.session.getCfHandleReadLock().unlock();
            } catch (Exception e) {
                log.error("rollback {}", e);
            }
        }
    }

    @Override
    public ScanIterator scan(String tableName) {
        try (CFHandleLock handle = this.getLock(tableName)) {
            if (handle == null) {
                log.info("no find table : {}", tableName);
                return null;
            }
            return new RocksDBScanIterator(this.rocksdb().newIterator(handle.get()), null, null,
                                           ScanIterator.Trait.SCAN_ANY,
                                           this.session.getRefCounter());
        }
    }

    @Override
    public ScanIterator scan(String tableName, byte[] prefix) {
        return scan(tableName, prefix, 0);
    }

    @Override
    public ScanIterator scan(String tableName, byte[] prefix, int scanType) {
        try (CFHandleLock handle = this.getLock(tableName)) {
            if (handle == null) {
                log.info("no find table: {} for scanning with prefix: {}", tableName,
                         new String(prefix));
                return null;
            }
            return new RocksDBScanIterator(this.rocksdb().newIterator(handle.get()), prefix, null,
                                           ScanIterator.Trait.SCAN_PREFIX_BEGIN | scanType,
                                           this.session.getRefCounter());
        }
    }

    @Override
    public ScanIterator scan(String tableName, byte[] keyFrom, byte[] keyTo, int scanType) {
        try (CFHandleLock handle = this.getLock(tableName)) {
            if (handle == null) {
                log.info("no find table: {}  for scantype: {}", tableName, scanType);
                return null;
            }
            return new RocksDBScanIterator(this.rocksdb().newIterator(handle.get()), keyFrom, keyTo,
                                           scanType,
                                           this.session.getRefCounter());
        }
    }

    /**
     * 遍历所有cf指定范围的数据
     * TODO: rocksdb7.x 不支持 setStartSeqNum，改为使用 Timestamp
     * refer: https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp
     */
    @Override
    public ScanIterator scanRaw(byte[] keyFrom, byte[] keyTo, long startSeqNum) {
        int kNumInternalBytes = 8;      //internal key 增加的8个字节后缀
        Snapshot snapshot = rocksdb().getSnapshot();
        Iterator<String> cfIterator = session.getTables().keySet().iterator();

        return new ScanIterator() {
            String cfName = null;

            @Override
            public boolean hasNext() {
                return cfIterator.hasNext();
            }

            @Override
            public boolean isValid() {
                return cfIterator.hasNext();
            }

            @Override
            public <T> T next() {
                RocksIterator iterator = null;
                ReadOptions readOptions = new ReadOptions()
                        .setSnapshot(snapshot);
                if (keyFrom != null) {
                    readOptions.setIterateLowerBound(new Slice(keyFrom));
                }
                if (keyTo != null) {
                    readOptions.setIterateUpperBound(new Slice(keyTo));
                }
                while (iterator == null && cfIterator.hasNext()) {
                    cfName = cfIterator.next();
                    try (CFHandleLock handle = getLock(cfName)) {
                        iterator = rocksdb().newIterator(handle.get(), readOptions);
                        iterator.seekToFirst();
                    }
                }
                if (iterator == null) {
                    return null;
                }
                RocksIterator finalIterator = iterator;
                return (T) new ScanIterator() {
                    private final ReadOptions holdReadOptions = readOptions;

                    @Override
                    public boolean hasNext() {
                        return finalIterator.isValid();
                    }

                    @Override
                    public boolean isValid() {
                        return finalIterator.isValid();
                    }

                    @Override
                    public <T> T next() {
                        byte[] key = finalIterator.key();
                        if (startSeqNum > 0) {
                            key = Arrays.copyOfRange(key, 0, key.length - kNumInternalBytes);
                        }
                        RocksDBSession.BackendColumn col =
                                RocksDBSession.BackendColumn.of(key, finalIterator.value());
                        finalIterator.next();
                        return (T) col;
                    }

                    @Override
                    public void close() {
                        finalIterator.close();
                        holdReadOptions.close();
                    }

                };
            }

            @Override
            public void close() {
                rocksdb().releaseSnapshot(snapshot);
            }

            @Override
            public byte[] position() {
                return cfName.getBytes(StandardCharsets.UTF_8);

            }
        };
    }

    @Override
    public long keyCount(byte[] start, byte[] end, String tableName) {
        ScanIterator it = scan(tableName, start, end, ScanIterator.Trait.SCAN_LT_END);
        return it.count();
    }

    @Override
    public long estimatedKeyCount(String tableName) throws DBStoreException {
        try {
            return this.rocksdb()
                       .getLongProperty(session.getCF(tableName), "rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    private WriteBatch getBatch() {
        if (this.batch == null) {
            this.batch = new WriteBatch();
        }
        return this.batch;
    }
}
