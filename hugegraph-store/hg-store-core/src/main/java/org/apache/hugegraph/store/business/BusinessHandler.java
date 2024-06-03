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

package org.apache.hugegraph.store.business;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.Graphpb;
import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.OpType;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.apache.hugegraph.store.metric.HgStoreMetric;
import org.apache.hugegraph.store.raft.HgStoreStateMachine;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.store.util.HgStoreException;
import org.rocksdb.Cache;
import org.rocksdb.MemoryUsageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface BusinessHandler extends DBSessionBuilder {

    Logger log = LoggerFactory.getLogger(HgStoreStateMachine.class);
    String tableUnknown = "unknown";
    String tableVertex = "g+v";
    String tableOutEdge = "g+oe";
    String tableInEdge = "g+ie";
    String tableIndex = "g+index";
    String tableTask = "g+task";
    String tableOlap = "g+olap";
    String tableServer = "g+server";

    String[] tables = new String[]{tableUnknown, tableVertex, tableOutEdge, tableInEdge, tableIndex,
                                   tableTask, tableOlap, tableServer};

    void doPut(String graph, int code, String table, byte[] key, byte[] value) throws
                                                                               HgStoreException;

    byte[] doGet(String graph, int code, String table, byte[] key) throws HgStoreException;

    ScanIterator scanAll(String graph, String table) throws HgStoreException;

    ScanIterator scanAll(String graph, String table, byte[] query) throws HgStoreException;

    ScanIterator scan(String graph, String table, int codeFrom, int codeTo) throws HgStoreException;

    ScanIterator scan(String graph, int code, String table, byte[] start, byte[] end,
                      int scanType) throws HgStoreException;

    ScanIterator scan(String graph, int code, String table, byte[] start, byte[] end, int scanType,
                      byte[] conditionQuery) throws HgStoreException;

    <T> GraphStoreIterator<T> scan(Graphpb.ScanPartitionRequest request);

    ScanIterator scanOriginal(Graphpb.ScanPartitionRequest request);

    ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix,
                            int scanType) throws HgStoreException;

    ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix) throws
                                                                                 HgStoreException;

    HgStoreMetric.Partition getPartitionMetric(String graph, int partId,
                                               boolean accurateCount) throws HgStoreException;

    void batchGet(String graph, String table, Supplier<HgPair<Integer, byte[]>> s,
                  Consumer<HgPair<byte[], byte[]>> c) throws HgStoreException;

    void truncate(String graph, int partId) throws HgStoreException;

    void flushAll();

    void closeAll();

    //
    Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(List<Cache> caches);

    List<Integer> getLeaderPartitionIds(String graph);

    HgStoreMetric.Graph getGraphMetric(String graph, int partId);

    void saveSnapshot(String snapshotPath, String graph, int partId) throws HgStoreException;

    void loadSnapshot(String snapshotPath, String graph, int partId, long version) throws
                                                                                   HgStoreException;

    long getLatestSequenceNumber(String graph, int partId);

    // 扫描分区从 seqnum 开始的 kv
    ScanIterator scanRaw(String graph, int partId, long seqNum) throws HgStoreException;

    void ingestSstFile(String graph, int partId, Map<byte[], List<String>> sstFiles) throws
                                                                                     HgStoreException;

    //提交分区分裂，删除旧数据
    // 删除分区数据
    boolean deletePartition(String graph, int partId);

    //清理分区，删除多余的数据
    boolean cleanPartition(String graph, int partId);

    boolean cleanPartition(String graph, int partId, long startKey, long endKey,
                           CleanType cleanType);

    //所有指定分区图的所有 table 名
    List<String> getTableNames(String graph, int partId);

    TxBuilder txBuilder(String graph, int partId);

    default void doBatch(String graph, int partId, List<BatchEntry> entryList) {
        BusinessHandler.TxBuilder builder = txBuilder(graph, partId);
        try {
            for (BatchEntry b : entryList) {
                Key start = b.getStartKey();
                String table = tables[b.getTable()];
                byte[] startKey = start.getKey().toByteArray();
                int number = b.getOpType().getNumber();
                if (number == OpType.OP_TYPE_PUT_VALUE) {
                    builder.put(start.getCode(), table, startKey, b.getValue().toByteArray());
                } else {
                    switch (number) {
                        case OpType.OP_TYPE_DEL_VALUE:
                            builder.del(start.getCode(), table, startKey);
                            continue;
                        case OpType.OP_TYPE_DEL_PREFIX_VALUE:
                            builder.delPrefix(start.getCode(), table, startKey);
                            continue;
                        case OpType.OP_TYPE_DEL_RANGE_VALUE:
                            builder.delRange(start.getCode(), table, startKey,
                                             b.getEndKey().getKey().toByteArray());
                            continue;
                        case OpType.OP_TYPE_DEL_SINGLE_VALUE:
                            builder.delSingle(start.getCode(), table, startKey);
                            continue;
                        case OpType.OP_TYPE_MERGE_VALUE:
                            builder.merge(start.getCode(), table, startKey,
                                          b.getValue().toByteArray());
                            continue;
                        default:
                            throw new IllegalArgumentException(
                                    "unsupported batch-op-type: " + b.getOpType().name());
                    }
                }
            }
            builder.build().commit();
        } catch (Throwable e) {
            String msg =
                    String.format("graph data %s-%s do batch insert with error:", graph, partId);
            log.error(msg, e);
            builder.build().rollback();
            throw e;
        }
    }

    boolean existsTable(String graph, int partId, String table);

    void createTable(String graph, int partId, String table);

    void deleteTable(String graph, int partId, String table);

    void dropTable(String graph, int partId, String table);

    boolean dbCompaction(String graphName, int partitionId);

    boolean dbCompaction(String graphName, int partitionId, String tableName);

    void destroyGraphDB(String graphName, int partId) throws HgStoreException;

    long count(String graphName, String table);

    @NotThreadSafe
    interface TxBuilder {

        TxBuilder put(int code, String table, byte[] key, byte[] value) throws HgStoreException;

        TxBuilder del(int code, String table, byte[] key) throws HgStoreException;

        TxBuilder delSingle(int code, String table, byte[] key) throws HgStoreException;

        TxBuilder delPrefix(int code, String table, byte[] prefix) throws HgStoreException;

        TxBuilder delRange(int code, String table, byte[] start, byte[] end) throws
                                                                             HgStoreException;

        TxBuilder merge(int code, String table, byte[] key, byte[] value) throws HgStoreException;

        Tx build();
    }

    interface Tx {

        void commit() throws HgStoreException;

        void rollback() throws HgStoreException;
    }
}
