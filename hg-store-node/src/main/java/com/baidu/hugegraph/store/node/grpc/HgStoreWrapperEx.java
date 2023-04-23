package com.baidu.hugegraph.store.node.grpc;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.business.BusinessHandler;
import com.baidu.hugegraph.store.business.FilterIterator;
import com.baidu.hugegraph.store.grpc.common.GraphMethod;
import com.baidu.hugegraph.store.grpc.common.TableMethod;
import com.baidu.hugegraph.store.grpc.session.BatchEntry;
import com.baidu.hugegraph.store.term.HgPair;

import lombok.extern.slf4j.Slf4j;

/**
 * @projectName: hugegraph-store
 * @package: com.baidu.hugegraph.store.grpc.boot.service
 * @className: HgStoreWrapperEx
 * @author: tyzer
 * @description: TODO
 * @date: 2021/10/25 18:35
 * @version: 1.0
 */
@Slf4j
public class HgStoreWrapperEx {

    private final BusinessHandler handler;

    public HgStoreWrapperEx(BusinessHandler handler) {
        this.handler = handler;
    }

    public byte[] doGet(String graph, int code, String table, byte[] key) {
        return this.handler.doGet(graph, code, table, key);
    }

    public boolean doClean(String graph, int partId) {
        return this.handler.cleanPartition(graph, partId);
    }

    public ScanIterator scanAll(String graph, String table, byte[] query) {
        ScanIterator scanIterator = this.handler.scanAll(graph, table, query);
        return FilterIterator.of(scanIterator, query);
    }

    public ScanIterator scan(String graph, int partId, String table, byte[] start, byte[] end, int scanType,
                             byte[] query) {
        ScanIterator scanIterator = this.handler.scan(graph, partId, table, start, end, scanType, query);
        return FilterIterator.of(scanIterator, query);
    }

    public void batchGet(String graph, String table, Supplier<HgPair<Integer, byte[]>> s,
                         Consumer<HgPair<byte[], byte[]>> c) {
        this.handler.batchGet(graph, table, s, (pair -> {
            c.accept(new HgPair<>(pair.getKey(), pair.getValue()));
        }));
    }

    public ScanIterator scanPrefix(String graph, int partition, String table, byte[] prefix, int scanType,
                                   byte[] query) {
        ScanIterator scanIterator = this.handler.scanPrefix(graph, partition, table, prefix, scanType);
        return FilterIterator.of(scanIterator, query);
    }

    public void doBatch(String graph, int partId, List<BatchEntry> entryList) {
        this.handler.doBatch(graph, partId, entryList);
    }

    public boolean doTable(int partId, TableMethod method, String graph, String table) {
        boolean flag;
        switch (method) {
            case TABLE_METHOD_EXISTS:
                flag = this.handler.existsTable(graph, partId, table);
                break;
            case TABLE_METHOD_CREATE:
                this.handler.createTable(graph, partId, table);
                flag = true;
                break;
            case TABLE_METHOD_DELETE:
                this.handler.deleteTable(graph, partId, table);
                flag = true;
                break;
            case TABLE_METHOD_DROP:
                this.handler.dropTable(graph, partId, table);
                flag = true;
                break;
            case TABLE_METHOD_TRUNCATE:
                this.handler.truncate(graph, partId);
                flag = true;
                break;
            default:
                throw new UnsupportedOperationException("TableMethod: " + method.name());
        }

        return flag;
    }

    public boolean doGraph(int partId, GraphMethod method, String graph) {
        boolean flag = true;
        switch (method) {
            case GRAPH_METHOD_DELETE:
                // 交给raft执行，此处不处理
                flag = true;
                break;
            default:
                throw new UnsupportedOperationException("GraphMethod: " + method.name());
        }
        return flag;
    }
}
