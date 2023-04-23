package com.baidu.hugegraph.rocksdb.access;

import com.baidu.hugegraph.store.term.HgPair;

public interface SessionOperator {

    HgPair<byte[], byte[]> keyRange(String table);

    void compactRange(String table) throws DBStoreException;
    void compactRange() throws DBStoreException;
    void put(String table, byte[] key, byte[] value) throws DBStoreException;

    ScanIterator scan(String tableName);
    ScanIterator scan(String tableName, byte[] prefix);
    ScanIterator scan(String tableName, byte[] prefix, int scanType);
    ScanIterator scan(String tableName, byte[] keyFrom, byte[] keyTo, int scanType);

    /**
     * 扫描所有cf指定范围的数据
     */
    ScanIterator scanRaw(byte[] keyFrom, byte[] keyTo, long startSeqNum);

    long keyCount(byte[] start, byte[] end, String tableName);

    long estimatedKeyCount(String tableName);

    /*
     * only support 'long data' operator
     * */
    void merge(String table, byte[] key, byte[] value) throws DBStoreException;

    void increase(String table, byte[] key, byte[] value) throws DBStoreException;

    void delete(String table, byte[] key) throws DBStoreException;

    void deleteSingle(String table, byte[] key) throws DBStoreException;

    void deletePrefix(String table, byte[] key) throws DBStoreException;

    void deleteRange(String table, byte[] keyFrom, byte[] keyTo) throws DBStoreException;
    /**
     * 删除所有cf指定范围的数据
     */
    void deleteRange(byte[] keyFrom, byte[] keyTo) throws DBStoreException;
    byte[] get(String table, byte[] key) throws DBStoreException;

    void prepare();
    Integer commit() throws DBStoreException;
    void rollback();
    RocksDBSession getDBSession();
}
