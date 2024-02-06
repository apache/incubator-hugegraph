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

import org.apache.hugegraph.store.term.HgPair;

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
