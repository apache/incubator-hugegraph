package com.baidu.hugegraph.store;

import com.baidu.hugegraph.store.client.grpc.KvCloseableIterator;
import com.baidu.hugegraph.store.grpc.common.Kv;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamReq;

import java.util.Iterator;
import java.util.List;

/**
 * @author lynn.bond@hotmail.com
 * @version 0.2.0
 */
public interface HgKvStore {
    /**
     * CAUTION: THE CONST BELOW MUST KEEP CONSISTENCE TO ScanIterator.Trait.
     */
    int SCAN_ANY = 0x80;
    int SCAN_PREFIX_BEGIN = 0x01;
    int SCAN_PREFIX_END = 0x02;
    int SCAN_GT_BEGIN = 0x04;
    int SCAN_GTE_BEGIN = 0x0c;
    int SCAN_LT_END = 0x10;
    int SCAN_LTE_END = 0x30;
    int SCAN_KEYONLY = 0x40;
    int SCAN_HASHCODE = 0x100;

    boolean put(String table, HgOwnerKey ownerKey, byte[] value);

    /**
     * 该版本被store内部使用。向分区写入数据，
     * partitionId与key.keyCode必须与pd存储的分区信息保持一致。
     */
    boolean directPut(String table, int partitionId,  HgOwnerKey key, byte[] value);
    byte[] get(String table, HgOwnerKey ownerKey);

    boolean clean(int partId);

    boolean delete(String table, HgOwnerKey ownerKey);

    boolean deleteSingle(String table, HgOwnerKey ownerKey);

    boolean deletePrefix(String table, HgOwnerKey prefix);

    boolean deleteRange(String table, HgOwnerKey start, HgOwnerKey end);

    boolean merge(String table, HgOwnerKey key, byte[] value);
    @Deprecated
    List<HgKvEntry> batchGetOwner(String table, List<HgOwnerKey> keyList);

    HgKvIterator<HgKvEntry> scanIterator(String table);

    HgKvIterator<HgKvEntry> scanIterator(String table, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, long limit);

    HgKvIterator<HgKvEntry> scanIterator(String table, long limit, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey, long limit);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey, long limit, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey, long limit, int scanType, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, int codeFrom, int codeTo, int scanType, byte[] query);

    // HgKvIterator<HgKvEntry> scanIterator(ScanStreamReq scanReq);

    HgKvIterator<HgKvEntry> scanIterator(ScanStreamReq.Builder scanReqBuilder);

    boolean truncate();

    default boolean existsTable(String table) {
        return false;
    }

    boolean createTable(String table);

    boolean deleteTable(String table);

    boolean dropTable(String table);

    boolean deleteGraph(String graph);
    List<HgKvIterator<HgKvEntry>> scanBatch(HgScanQuery scanQuery);

    KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch2(HgScanQuery scanQuery);
    KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch3(HgScanQuery scanQuery, KvCloseableIterator iterator);

    HgKvIterator<HgKvEntry> batchPrefix(String table, List<HgOwnerKey> prefixList);
}

