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

import static org.apache.hugegraph.store.util.HgStoreConst.EMPTY_BYTES;
import static org.apache.hugegraph.store.util.HgStoreConst.SCAN_ALL_PARTITIONS_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.rocksdb.access.DBStoreException;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.cmd.CleanDataRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Request;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.ScanType;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.asynctask.AsyncTaskState;
import org.apache.hugegraph.store.meta.asynctask.CleanTask;
import org.apache.hugegraph.store.metric.HgStoreMetric;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.store.util.HgStoreException;
import org.rocksdb.Cache;
import org.rocksdb.MemoryUsageType;

import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BusinessHandlerImpl implements BusinessHandler {

    private static final int batchSize = 10000;
    private static final RocksDBFactory factory = RocksDBFactory.getInstance();
    private static final HashMap<ScanType, String> tableMapping = new HashMap<>() {{
        put(ScanType.SCAN_VERTEX, tableVertex);
        put(ScanType.SCAN_EDGE, tableOutEdge);
    }};
    private static final Map<Integer, String> dbNames = new ConcurrentHashMap<>();

    static {
        int code = tableUnknown.hashCode();
        code = tableVertex.hashCode();
        code = tableOutEdge.hashCode();
        code = tableInEdge.hashCode();
        code = tableIndex.hashCode();
        code = tableTask.hashCode();
        code = tableTask.hashCode();
        log.debug("init table code:{}", code);
    }

    private final PartitionManager partitionManager;
    private final PdProvider provider;
    private final InnerKeyCreator keyCreator;

    public BusinessHandlerImpl(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
        this.provider = partitionManager.getPdProvider();
        this.keyCreator = new InnerKeyCreator(this);

        factory.addRocksdbChangedListener(new RocksdbChangedListener() {
            @Override
            public void onDBDeleteBegin(String dbName, String filePath) {
                partitionManager.getDeletedFileManager().addDeletedFile(filePath);
            }

            @Override
            public void onDBDeleted(String dbName, String filePath) {
                partitionManager.getDeletedFileManager().removeDeletedFile(filePath);
            }

            @Override
            public void onDBSessionReleased(RocksDBSession dbSession) {

            }
        });
    }

    public static HugeConfig initRocksdb(Map<String, Object> rocksdbConfig,
                                         RocksdbChangedListener listener) {
        // 注册 rocksdb 配置
        OptionSpace.register("rocksdb", "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        HugeConfig hConfig = new HugeConfig(new MapConfiguration(rocksdbConfig));
        factory.setHugeConfig(hConfig);
        if (listener != null) {
            factory.addRocksdbChangedListener(listener);
        }
        return hConfig;
    }

    public static String getDbName(int partId) {
        String dbName = dbNames.get(partId);
        if (dbName == null) {
            dbName = String.format("%05d", partId);
            dbNames.put(partId, dbName);
        }
        // 每个分区对应一个 rocksdb 实例，因此 rocksdb 实例名为 partId
        return dbName;
    }

    @Override
    public void doPut(String graph, int code, String table, byte[] key, byte[] value) throws
                                                                                      HgStoreException {

        int partId = provider.getPartitionByCode(graph, code).getId();
        try (RocksDBSession dbSession = getSession(graph, table, partId)) {
            SessionOperator op = dbSession.sessionOp();
            try {
                op.prepare();
                byte[] targetKey = keyCreator.getKey(partId, graph, code, key);
                op.put(table, targetKey, value);
                op.commit();
            } catch (Exception e) {
                log.error("Graph " + graph + " doPut exception", e);
                op.rollback();
                throw new HgStoreException(HgStoreException.EC_RKDB_DOPUT_FAIL, e.toString());
            }
        }
    }

    @Override
    public byte[] doGet(String graph, int code, String table, byte[] key) throws HgStoreException {
        int partId = provider.getPartitionByCode(graph, code).getId();

        try (RocksDBSession dbSession = getSession(graph, table, partId)) {
            byte[] targetKey = keyCreator.getKey(partId, graph, code, key);
            return dbSession.sessionOp().get(table, targetKey);
        } catch (Exception e) {
            log.error("Graph " + graph + " doGet exception", e);
            throw new HgStoreException(HgStoreException.EC_RKDB_DOGET_FAIL, e.toString());
        }
    }

    @Override
    public ScanIterator scanAll(String graph, String table) throws HgStoreException {
        List<Integer> ids = this.getLeaderPartitionIds(graph);

        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                return new InnerKeyFilter(dbSession.sessionOp().scan(table, position == null ?
                                                                            keyCreator.getStartKey(
                                                                                    id, graph) :
                                                                            keyCreator.getStartKey(
                                                                                    id, graph,
                                                                                    position),
                                                                     keyCreator.getEndKey(id,
                                                                                          graph),
                                                                     ScanIterator.Trait.SCAN_LT_END));
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    @Override
    public ScanIterator scanAll(String graph, String table, byte[] query) throws HgStoreException {
        return scanAll(graph, table);
    }

    @Override
    public ScanIterator scan(String graph, int code, String table, byte[] start, byte[] end,
                             int scanType) throws HgStoreException {
        List<Integer> ids;
        if (code == SCAN_ALL_PARTITIONS_ID) {
            ids = this.getLeaderPartitionIds(graph);
        } else {
            ids = new ArrayList<>();
            ids.add(partitionManager.getPartitionIdByCode(graph, code));
        }
        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            byte[] endKey;
            int type;
            if (ArrayUtils.isEmpty(end)) {
                endKey = keyCreator.getEndKey(id, graph);
                type = ScanIterator.Trait.SCAN_LT_END;
            } else {
                endKey = keyCreator.getEndKey(id, graph, end);
                type = scanType;
            }
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                return new InnerKeyFilter(dbSession.sessionOp().scan(table,
                                                                     keyCreator.getStartKey(id,
                                                                                            graph,
                                                                                            toPosition(
                                                                                                    start,
                                                                                                    position)),
                                                                     endKey, type));
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    /**
     * 根据 keyCode 范围返回数据，左闭右开
     *
     * @param graph
     * @param table
     * @param codeFrom 起始 code，包含该值
     * @param codeTo   结束 code，不包含该值
     * @return
     * @throws HgStoreException
     */
    @Override
    public ScanIterator scan(String graph, String table, int codeFrom, int codeTo) throws
                                                                                   HgStoreException {

        List<Integer> ids = new ArrayList<>();
        ids.add(partitionManager.getPartitionIdByCode(graph, codeFrom));
        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                byte[] startKey;
                if (position != null) {
                    startKey = keyCreator.getStartKey(id, graph, position);
                } else {
                    startKey = keyCreator.getStartKey(id, graph);
                }
                byte[] endKey = keyCreator.getEndKey(id, graph);
                ScanIterator iterator = dbSession.sessionOp().scan(table, startKey, endKey,
                                                                   ScanIterator.Trait.SCAN_LT_END);
                return new InnerKeyFilter(iterator, codeFrom, codeTo);
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    @Override
    public ScanIterator scan(String graph, int code, String table, byte[] start, byte[] end,
                             int scanType, byte[] conditionQuery) throws HgStoreException {
        ScanIterator it = null;
        if ((scanType & ScanIterator.Trait.SCAN_HASHCODE) == ScanIterator.Trait.SCAN_HASHCODE) {
            int codeFrom = Bits.toInt(start);
            int codeTo = Bits.toInt(end);
            it = scan(graph, table, codeFrom, codeTo);
        } else {
            it = scan(graph, code, table, start, end, scanType);
        }
        return it;
    }

    @Override
    public GraphStoreIterator scan(ScanPartitionRequest spr) throws HgStoreException {
        return new GraphStoreIterator(scanOriginal(spr), spr);
    }

    @Override
    public ScanIterator scanOriginal(ScanPartitionRequest spr) throws HgStoreException {
        Request request = spr.getScanRequest();
        String graph = request.getGraphName();
        List<Integer> ids;
        int partitionId = request.getPartitionId();
        int startCode = request.getStartCode();
        int endCode = request.getEndCode();
        if (partitionId == SCAN_ALL_PARTITIONS_ID) {
            ids = this.getLeaderPartitionIds(graph);
        } else {
            ids = new ArrayList<>();
            if (startCode != 0 || endCode != 0) {
                ids.add(partitionManager.getPartitionIdByCode(graph, startCode));
            } else {
                ids.add(partitionId);
            }
        }
        String table = request.getTable();
        if (StringUtils.isEmpty(table)) {
            table = tableMapping.get(request.getScanType());
        }
        int scanType = request.getBoundary();
        if (scanType == 0) {
            scanType = ScanIterator.Trait.SCAN_LT_END;
        }
        String tab = table;
        int st = scanType;
        BiFunction<Integer, byte[], ScanIterator> func = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, tab, id)) {
                byte[] startPos = toPosition(EMPTY_BYTES, position);
                byte[] startKey = keyCreator.getStartKey(id, graph, startPos);
                byte[] endKey = keyCreator.getEndKey(id, graph);
                ScanIterator iter = dbSession.sessionOp().scan(tab, startKey, endKey, st);
                return new InnerKeyFilter(iter);
            }
        };
        return MultiPartitionIterator.of(ids, func);
    }

    @Override
    public ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix,
                                   int scanType) throws HgStoreException {
        List<Integer> ids;
        if (code == SCAN_ALL_PARTITIONS_ID) {
            ids = this.getLeaderPartitionIds(graph);
        } else {
            ids = new ArrayList<>();
            ids.add(partitionManager.getPartitionIdByCode(graph, code));
        }
        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                return new InnerKeyFilter(dbSession.sessionOp().scan(table,
                                                                     keyCreator.getPrefixKey(id,
                                                                                             graph,
                                                                                             toPosition(
                                                                                                     prefix,
                                                                                                     position)),
                                                                     scanType));
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    @Override
    public ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix) throws
                                                                                        HgStoreException {

        return scanPrefix(graph, code, table, prefix, 0);
    }

    private byte[] toPosition(byte[] start, byte[] position) {
        if (position == null || position.length == 0) {
            return start;
        }
        return position;
    }

    @Override
    public HgStoreMetric.Partition getPartitionMetric(String graph, int partId,
                                                      boolean accurateCount) throws
                                                                             HgStoreException {
        // get key count
        Map<String, Long> countMap = null;
        Map<String, String> sizeMap = null;

        try (RocksDBSession dbSession = getSession(graph, partId)) {
            countMap = dbSession.getKeyCountPerCF(keyCreator.getStartKey(partId, graph),
                                                  keyCreator.getEndKey(partId, graph),
                                                  accurateCount);
            sizeMap = dbSession.getApproximateCFDataSize(keyCreator.getStartKey(partId, graph),
                                                         keyCreator.getEndKey(partId, graph));

            HgStoreMetric.Partition partMetric = new HgStoreMetric.Partition();
            partMetric.setPartitionId(partId);

            List<HgStoreMetric.Table> tables = new ArrayList<>(sizeMap.size());
            for (String tableName : sizeMap.keySet()) {
                HgStoreMetric.Table table = new HgStoreMetric.Table();
                table.setTableName(tableName);
                table.setKeyCount(countMap.get(tableName));
                table.setDataSize(sizeMap.get(tableName));
                tables.add(table);
            }

            partMetric.setTables(tables);
            return partMetric;
        }
    }

    @Override
    public HgStoreMetric.Graph getGraphMetric(String graph, int partId) {
        HgStoreMetric.Graph graphMetric = new HgStoreMetric.Graph();
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            graphMetric.setApproxDataSize(
                    dbSession.getApproximateDataSize(keyCreator.getStartKey(partId, graph),
                                                     keyCreator.getEndKey(partId, graph)));
            graphMetric.setApproxKeyCount(dbSession.getEstimateNumKeys());

            return graphMetric;
        }
    }

    @Override
    public void batchGet(String graph, String table, Supplier<HgPair<Integer, byte[]>> s,
                         Consumer<HgPair<byte[], byte[]>> c) throws HgStoreException {

        int count = 0;
        while (true) {
            // Prevent dead loops
            if (count++ == Integer.MAX_VALUE) {
                break;
            }
            HgPair<Integer, byte[]> duality = s.get();
            if (duality == null) {
                break;
            }
            int code = duality.getKey();
            byte[] key = duality.getValue();

            int partId = provider.getPartitionByCode(graph, code).getId();

            try (RocksDBSession dbSession = getSession(graph, table, partId)) {
                byte[] targetKey = keyCreator.getKey(partId, graph, code, key);
                byte[] value = dbSession.sessionOp().get(table, targetKey);
                c.accept(new HgPair<>(key, value));
            }

        }
    }

    /**
     * 清空图数据
     */
    @Override
    public void truncate(String graphName, int partId) throws HgStoreException {
        // 每个分区对应一个 rocksdb 实例，因此 rocksdb 实例名为 rocksdb + partId
        try (RocksDBSession dbSession = getSession(graphName, partId)) {
            dbSession.sessionOp().deleteRange(keyCreator.getStartKey(partId, graphName),
                                              keyCreator.getEndKey(partId, graphName));
            // 释放图 ID
            keyCreator.delGraphId(partId, graphName);
        }
    }

    @Override
    public void flushAll() {
        log.warn("Flush all!!! ");
        factory.getGraphNames().forEach(dbName -> {
            try (RocksDBSession dbSession = factory.queryGraphDB(dbName)) {
                if (dbSession != null) {
                    dbSession.flush(false);
                }
            }
        });
    }

    @Override
    public void closeAll() {
        log.warn("close all db!!! ");
        factory.getGraphNames().forEach(dbName -> {
            factory.releaseGraphDB(dbName);
        });
    }

    @Override
    public Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(List<Cache> caches) {
        try {
            return factory.getApproximateMemoryUsageByType(null, caches);
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    @Override
    public List<Integer> getLeaderPartitionIds(String graph) {
        return partitionManager.getLeaderPartitionIds(graph);
    }

    @Override
    public void saveSnapshot(String snapshotPath, String graph, int partId) throws
                                                                            HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            dbSession.saveSnapshot(snapshotPath);
        } catch (DBStoreException e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_EXPORT_SNAPSHOT_FAIL, e.toString());
        }
    }

    @Override
    public void loadSnapshot(String snapshotPath, String graph, int partId, long v1) throws
                                                                                     HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            dbSession.loadSnapshot(snapshotPath, v1);
            keyCreator.clearCache(partId);
            factory.destroyGraphDB(dbSession.getGraphName());
        } catch (DBStoreException e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_IMPORT_SNAPSHOT_FAIL, e.toString());
        }
    }

    @Override
    public long getLatestSequenceNumber(String graph, int partId) {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            return dbSession.getLatestSequenceNumber();
        }
    }

    @Override
    public ScanIterator scanRaw(String graph, int partId, long seqNum) throws HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            return dbSession.sessionOp().scanRaw(null, null, seqNum);
        } catch (DBStoreException e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_EXPORT_SNAPSHOT_FAIL, e.toString());
        }
    }

    @Override
    public void ingestSstFile(String graph, int partId, Map<byte[], List<String>> sstFiles) throws
                                                                                            HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            dbSession.ingestSstFile(sstFiles);
        }
    }

    @Override
    public boolean cleanPartition(String graph, int partId) {
        Partition partition = partitionManager.getPartitionFromPD(graph, partId);
        cleanPartition(graph, partId, partition.getStartKey(), partition.getEndKey(),
                       CleanType.CLEAN_TYPE_KEEP_RANGE);
        return true;
    }

    @Override
    public boolean cleanPartition(String graph, int partId, long startKey, long endKey,
                                  CleanType cleanType) {
        Partition partition = partitionManager.getPartition(graph, partId);
        if (partition == null) {
            return true;
        }

        log.info("cleanPartition: graph {}, part id: {}, {} -> {}, cleanType:{}", graph, partId,
                 startKey, endKey, cleanType);

        var taskManager = HgStoreEngine.getInstance().getPartitionEngine(partId).getTaskManager();
        CleanDataRequest request = new CleanDataRequest();
        request.setPartitionId(partId);
        request.setGraphName(graph);
        request.setKeyStart(startKey);
        request.setKeyEnd(endKey);
        request.setCleanType(cleanType);
        var cleanTask = new CleanTask(partId, graph, AsyncTaskState.START, request);
        taskManager.putAsyncTask(cleanTask);

        Utils.runInThread(() -> {
            cleanPartition(partition, code -> {
                // in range
                boolean flag = code >= startKey && code < endKey;
                return (cleanType == CleanType.CLEAN_TYPE_KEEP_RANGE) == flag;
            });
            // 可能被 destroy 了
            if (HgStoreEngine.getInstance().getPartitionEngine(partId) != null) {
                taskManager.updateAsyncTaskState(partId, graph, cleanTask.getId(),
                                                 AsyncTaskState.SUCCESS);
            }
        });
        return true;
    }

    /**
     * 清理分区数据，删除非本分区的数据
     * 遍历 partId 的所有 key，读取 code，if code >= splitKey 生成新的 key，写入 newPartId
     */
    private boolean cleanPartition(Partition partition,
                                   Function<Integer, Boolean> belongsFunction) {
        log.info("Partition {}-{} cleanPartition begin... {}", partition.getGraphName(),
                 partition.getId(), partition);
        int counter = 0;

        SessionOperator op = getSession(partition.getGraphName(), partition.getId()).sessionOp();
        try {
            ScanIterator cfIterator =
                    op.scanRaw(keyCreator.getStartKey(partition.getId(), partition.getGraphName()),
                               keyCreator.getEndKey(partition.getId(), partition.getGraphName()),
                               0);
            while (cfIterator.hasNext()) {
                ScanIterator iterator = cfIterator.next();
                String table = new String(cfIterator.position());
                long deleted = 0;
                long total = 0;
                while (iterator.hasNext()) {
                    total += 1;
                    RocksDBSession.BackendColumn col = iterator.next();
                    int keyCode = keyCreator.parseKeyCode(col.name);
                    // if (keyCode < partition.getStartKey() || keyCode >= partition.getEndKey()) {
                    if (!belongsFunction.apply(keyCode)) {
                        if (counter == 0) {
                            op.prepare();
                        }
                        op.delete(table, col.name); // 删除旧数据
                        if (++counter > batchSize) {
                            op.commit();
                            counter = 0;
                        }
                        deleted += 1;
                    }
                }
                iterator.close();
                log.info("partition {}-{}, table:{}, delete keys {}, total:{}",
                         partition.getGraphName(), partition.getId(), table, deleted, total);
            }
            cfIterator.close();
        } catch (Exception e) {
            log.error("Partition {}-{} cleanPartition exception {}", partition.getGraphName(),
                      partition.getId(), e);
            op.rollback();
            throw e;
        } finally {
            if (counter > 0) {
                try {
                    op.commit();
                } catch (Exception e) {
                    op.rollback();
                    throw e;
                }
            }
            op.getDBSession().close();
        }
        op.compactRange();
        log.info("Partition {}-{} cleanPartition end", partition.getGraphName(), partition.getId());
        return true;
    }

    @Override
    public boolean deletePartition(String graph, int partId) {
        try {
            deleteGraphDatabase(graph, partId);
        } catch (Exception e) {
            log.error("Partition {}-{} deletePartition exception {}", graph, partId, e);
        }
        return true;
    }

    @Override
    public List<String> getTableNames(String graph, int partId) {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            List<String> tables = null;
            tables = dbSession.getTables().keySet().stream().collect(Collectors.toList());
            return tables;
        }
    }

    private RocksDBSession getSession(String graph, String table, int partId) throws
                                                                              HgStoreException {
        RocksDBSession dbSession = getSession(partId);
        dbSession.checkTable(table);
        return dbSession;
    }

    private RocksDBSession getSession(String graphName, int partId) throws HgStoreException {
        return getSession(partId);
    }

    /**
     * 获取 dbsession，不更新 dbsession 活跃时间
     */
    @Override
    public RocksDBSession getSession(int partId) throws HgStoreException {
        // 每个分区对应一个 rocksdb 实例，因此 rocksdb 实例名为 rocksdb + partId
        String dbName = getDbName(partId);
        RocksDBSession dbSession = factory.queryGraphDB(dbName);
        if (dbSession == null) {
            long version = HgStoreEngine.getInstance().getCommittedIndex(partId);
            dbSession =
                    factory.createGraphDB(partitionManager.getDbDataPath(partId, dbName), dbName,
                                          version);
            if (dbSession == null) {
                log.info("failed to create a new graph db: {}", dbName);
                throw new HgStoreException(HgStoreException.EC_RKDB_CREATE_FAIL,
                                           "failed to create a new graph db: {}", dbName);
            }
        }
        dbSession.setDisableWAL(true); //raft 模式，关闭 rocksdb 日志
        return dbSession;
    }

    private void deleteGraphDatabase(String graph, int partId) throws IOException {
        truncate(graph, partId);
    }

    private PartitionManager getPartManager() {
        return this.partitionManager;
    }

    @Override
    public TxBuilder txBuilder(String graph, int partId) throws HgStoreException {
        return new TxBuilderImpl(graph, partId, getSession(graph, partId));
    }

    @Override
    public boolean existsTable(String graph, int partId, String table) {
        try (RocksDBSession session = getSession(graph, partId)) {
            return session.tableIsExist(table);
        }
    }

    @Override
    public void createTable(String graph, int partId, String table) {
        try (RocksDBSession session = getSession(graph, partId)) {
            session.checkTable(table);
        }
    }

    @Override
    public void deleteTable(String graph, int partId, String table) {
        dropTable(graph, partId, table);
        // todo 检查表是否为空，为空则真实删除表
//        try (RocksDBSession session = getOrCreateGraphDB(graph, partId)) {
//            session.deleteTables(table);
//        }
    }

    @Override
    public void dropTable(String graph, int partId, String table) {
        try (RocksDBSession session = getSession(graph, partId)) {
//            session.dropTables(table);
            session.sessionOp().deleteRange(table, keyCreator.getStartKey(partId, graph),
                                            keyCreator.getEndKey(partId, graph));
        }
    }

    /**
     * 对 rocksdb 进行 compaction
     */
    @Override
    public boolean dbCompaction(String graphName, int partitionId) {
        return this.dbCompaction(graphName, partitionId, "");
    }

    /**
     * 对 rocksdb 进行 compaction
     */
    @Override
    public boolean dbCompaction(String graphName, int partitionId, String tableName) {
        try (RocksDBSession session = getSession(graphName, partitionId)) {
            SessionOperator op = session.sessionOp();
            if (tableName.isEmpty()) {
                op.compactRange();
            } else {
                op.compactRange(tableName);
            }
        }

        log.info("Partition {}-{} dbCompaction end", graphName, partitionId);
        return true;
    }

    /**
     * 销毁图，并删除数据文件
     *
     * @param graphName
     * @param partId
     */
    @Override
    public void destroyGraphDB(String graphName, int partId) throws HgStoreException {
        // 每个图每个分区对应一个 rocksdb 实例，因此 rocksdb 实例名为 rocksdb + partId
        String dbName = getDbName(partId);

        factory.destroyGraphDB(dbName);
        keyCreator.clearCache(partId);
    }

    @Override
    public long count(String graph, String table) {
        List<Integer> ids = this.getLeaderPartitionIds(graph);
        Long all = ids.parallelStream().map((id) -> {
            InnerKeyFilter it = null;
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                long count = 0;
                SessionOperator op = dbSession.sessionOp();
                it = new InnerKeyFilter(op.scan(table, keyCreator.getStartKey(id, graph),
                                                keyCreator.getEndKey(id, graph),
                                                ScanIterator.Trait.SCAN_LT_END));
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                return count;
            } catch (Exception e) {
                throw e;
            } finally {
                if (it != null) {
                    try {
                        it.close();
                    } catch (Exception e) {

                    }
                }
            }
        }).collect(Collectors.summingLong(l -> l));
        return all;
    }

    @NotThreadSafe
    private class TxBuilderImpl implements TxBuilder {

        private final String graph;
        private final int partId;
        private final RocksDBSession dbSession;
        private final SessionOperator op;

        private TxBuilderImpl(String graph, int partId, RocksDBSession dbSession) {
            this.graph = graph;
            this.partId = partId;
            this.dbSession = dbSession;
            this.op = this.dbSession.sessionOp();
            this.op.prepare();
        }

        @Override
        public TxBuilder put(int code, String table, byte[] key, byte[] value) throws
                                                                               HgStoreException {
            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                this.op.put(table, targetKey, value);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DOPUT_FAIL, e.toString());
            }
            return this;
        }

        @Override
        public TxBuilder del(int code, String table, byte[] key) throws HgStoreException {
            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                this.op.delete(table, targetKey);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DODEL_FAIL, e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder delSingle(int code, String table, byte[] key) throws HgStoreException {

            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                op.deleteSingle(table, targetKey);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RDKDB_DOSINGLEDEL_FAIL,
                                           e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder delPrefix(int code, String table, byte[] prefix) throws HgStoreException {

            try {
                this.op.deletePrefix(table, keyCreator.getPrefixKey(this.partId, graph, prefix));
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DODELPREFIX_FAIL, e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder delRange(int code, String table, byte[] start, byte[] end) throws
                                                                                    HgStoreException {

            try {
                this.op.deleteRange(table, keyCreator.getStartKey(this.partId, graph, start),
                                    keyCreator.getEndKey(this.partId, graph, end));
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DODELRANGE_FAIL, e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder merge(int code, String table, byte[] key, byte[] value) throws
                                                                                 HgStoreException {

            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                op.merge(table, targetKey, value);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DOMERGE_FAIL, e.toString());
            }
            return this;
        }

        @Override
        public Tx build() {
            return new Tx() {
                @Override
                public void commit() throws HgStoreException {
                    op.commit();  // commit发生异常后，必须调用rollback，否则造成锁未释放
                    dbSession.close();
                }

                @Override
                public void rollback() throws HgStoreException {
                    try {
                        op.rollback();
                    } finally {
                        dbSession.close();
                    }
                }
            };
        }
    }
}
