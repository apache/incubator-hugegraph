package com.baidu.hugegraph.store.meta.base;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import com.baidu.hugegraph.store.util.HgStoreException;
import com.baidu.hugegraph.store.util.Asserts;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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
                    ScanIterator.Trait.SCAN_GTE_BEGIN | ScanIterator.Trait.SCAN_LT_END);
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
                    ScanIterator.Trait.SCAN_GTE_BEGIN | ScanIterator.Trait.SCAN_LT_END);
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
