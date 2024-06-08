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

package org.apache.hugegraph.store.meta.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.util.Asserts;
import org.apache.hugegraph.store.util.HgStoreException;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Store、Partition等元数据存储到hgstore-metadata图下
 */
public abstract class MetaStoreBase implements Closeable {

    protected abstract RocksDBSession getRocksDBSession();

    protected abstract String getCFName();

    @Override
    public void close() throws IOException {
    }

    public void put(byte[] key, byte[] value) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            Asserts.isTrue(dbSession != null, "DB session is null.");
            SessionOperator op = dbSession.sessionOp();
            try {
                op.prepare();
                op.put(getCFName(), key, value);
                op.commit();
            } catch (Exception e) {
                op.rollback();
                throw e;
            }
        }
    }

    public void put(byte[] key, GeneratedMessageV3 value) {
        put(key, value.toByteArray());
    }

    public byte[] get(byte[] key) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator op = dbSession.sessionOp();
            return op.get(getCFName(), key);
        }
    }

    public <E> E get(com.google.protobuf.Parser<E> parser, byte[] key) {
        byte[] value = get(key);
        try {
            if (value != null) {
                return parser.parseFrom(value);
            }
        } catch (Exception e) {
            throw new HgStoreException(HgStoreException.EC_FAIL, e);
        }
        return null;
    }

    public List<RocksDBSession.BackendColumn> scan(byte[] prefix) {
        List<RocksDBSession.BackendColumn> values = new LinkedList<>();
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator op = dbSession.sessionOp();
            ScanIterator iterator = op.scan(getCFName(), prefix);
            while (iterator.hasNext()) {
                values.add(iterator.next());
            }
        }
        return values;
    }

    public <E> List<E> scan(com.google.protobuf.Parser<E> parser, byte[] prefix) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator op = dbSession.sessionOp();
            ScanIterator iterator = op.scan(getCFName(), prefix);
            List<E> values = new LinkedList<>();
            try {
                while (iterator.hasNext()) {
                    RocksDBSession.BackendColumn col = iterator.next();
                    values.add(parser.parseFrom(col.value));
                }
            } catch (InvalidProtocolBufferException e) {
                throw new HgStoreException(HgStoreException.EC_FAIL, e);
            }
            return values;
        }
    }

    public List<RocksDBSession.BackendColumn> scan(byte[] start, byte[] end) {
        List<RocksDBSession.BackendColumn> values = new LinkedList<>();
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator op = dbSession.sessionOp();
            ScanIterator iterator = op.scan(getCFName(), start, end,
                                            ScanIterator.Trait.SCAN_GTE_BEGIN |
                                            ScanIterator.Trait.SCAN_LT_END);
            while (iterator.hasNext()) {
                values.add(iterator.next());
            }
        }
        return values;
    }

    public <E> List<E> scan(com.google.protobuf.Parser<E> parser, byte[] start, byte[] end) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator op = dbSession.sessionOp();
            ScanIterator iterator = op.scan(getCFName(), start, end,
                                            ScanIterator.Trait.SCAN_GTE_BEGIN |
                                            ScanIterator.Trait.SCAN_LT_END);
            List<E> values = new LinkedList<>();
            try {
                while (iterator.hasNext()) {
                    RocksDBSession.BackendColumn col = iterator.next();
                    values.add(parser.parseFrom(col.value));
                }
            } catch (InvalidProtocolBufferException e) {
                throw new HgStoreException(HgStoreException.EC_FAIL, e);
            }
            return values;
        }
    }

    public void delete(byte[] key) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator op = dbSession.sessionOp();
            try {
                op.prepare();
                op.delete(getCFName(), key);
                op.commit();
            } catch (Exception e) {
                op.rollback();
                throw e;
            }
        }
    }

    public void deletePrefix(byte[] key) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator op = dbSession.sessionOp();
            try {
                op.prepare();
                op.deletePrefix(getCFName(), key);
                op.commit();
            } catch (Exception e) {
                op.rollback();
                throw e;
            }
        }
    }
}
