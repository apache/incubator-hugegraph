package com.baidu.hugegraph.store.node.grpc;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.baidu.hugegraph.pd.common.KVPair;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.business.SelectIterator;
import com.baidu.hugegraph.store.grpc.common.ScanMethod;
import com.baidu.hugegraph.store.grpc.stream.ScanQueryRequest;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamReq;
import com.baidu.hugegraph.store.grpc.stream.SelectParam;
import com.baidu.hugegraph.store.node.util.HgStoreNodeUtil;

import lombok.extern.slf4j.Slf4j;


/**
 * @author lynn.bond@hotmail.com created on 2022/02/22
 * @version 1.0.0
 */
@Slf4j
class ScanUtil {

    private final static Map<String, byte[]> tableKeyMap = new HashMap<>();


    static ScanIterator getIterator(ScanStreamReq request, HgStoreWrapperEx wrapper) {
        String graph = request.getHeader().getGraph();
        String table = request.getTable();
        ScanMethod method = request.getMethod();
        byte[] start = request.getStart().toByteArray();
        byte[] end = request.getEnd().toByteArray();
        byte[] prefix = request.getPrefix().toByteArray();
        int partition = request.getCode();
        int scanType = request.getScanType();
        byte[] query = request.getQuery().toByteArray();

        ScanIterator iter = null;
        switch (method) {
            case ALL:
                iter = wrapper.scanAll(graph, table, query);
                break;
            case PREFIX:
                iter = wrapper.scanPrefix(graph, partition, table, prefix, scanType, query);
                break;
            case RANGE:
                iter = wrapper.scan(graph, partition, table, start, end, scanType, query);
                break;
        }
        if (iter == null) {
            log.warn("Failed to create a scanIterator with ScanMethod: [" + method + "]");
            iter = new EmptyIterator();
        }
        SelectParam selects = request.getSelects();
        List<Integer> properties = null;
        if (selects != null) {
            properties = selects.getPropertiesList();
        }
        iter = new SelectIterator(iter, properties);
        iter.seek(request.getPosition().toByteArray());
        return iter;
    }

    static ScanIterator getIterator(ScanQuery sq, HgStoreWrapperEx wrapper) {
        if (log.isDebugEnabled()) log.debug("{}", sq);

        ScanIterator iter = null;
        switch (sq.method) {
            case ALL:
                iter = wrapper.scanAll(sq.graph, sq.table, sq.query);
                break;
            case PREFIX:
                iter = wrapper.scanPrefix(sq.graph, sq.keyCode, sq.table, sq.prefix, sq.scanType, sq.query);
                break;
            case RANGE:
                iter = wrapper.scan(sq.graph, sq.keyCode, sq.table, sq.start, sq.end, sq.scanType, sq.query);
                break;
        }

        if (iter == null) {
            log.warn("Failed to create a scanIterator with ScanMethod: [" + sq.method + "]");
            iter = new EmptyIterator();
        }

        iter.seek(sq.position);

        return iter;

    }

    static ScanQuery toSq(ScanStreamReq request) {
        ScanQuery res = ScanQuery.of();

        res.graph = request.getHeader().getGraph();
        res.table = request.getTable();
        res.method = request.getMethod();

        res.keyCode = request.getCode();
        res.start = request.getStart().toByteArray();
        res.end = request.getEnd().toByteArray();
        res.prefix = request.getPrefix().toByteArray();
        res.scanType = request.getScanType();
        res.query = request.getQuery().toByteArray();
        res.position = request.getPosition().toByteArray();

        if (log.isDebugEnabled()) log.debug("{}", res);
        //TODO: removed below.

        return res;
    }

    static ScanIterator getIterator(String graph, ScanQueryRequest request, HgStoreWrapperEx wrapper) {
        ScanIteratorSupplier supplier = new ScanIteratorSupplier(graph, request, wrapper);
        return BatchScanIterator.of(supplier, supplier.getLimitSupplier());
    }

    /**
     * 支持并行读取的多迭代器
     */
    static ScanIterator getParallelIterator(String graph, ScanQueryRequest request,
                                            HgStoreWrapperEx wrapper, ThreadPoolExecutor executor) {
        ScanIteratorSupplier supplier = new ScanIteratorSupplier(graph, request, wrapper);
        return ParallelScanIterator.of(supplier, supplier.getLimitSupplier(),
                request, executor);
    }

    @NotThreadSafe
    private static class ScanIteratorSupplier implements Supplier<KVPair<QueryCondition, ScanIterator>> {

        private AtomicBoolean isEmpty = new AtomicBoolean();

        private String graph;
        private long perKeyLimit;
        private long perKeyMax;
        private long skipDegree;
        private HgStoreWrapperEx wrapper;

        private List<ScanQuery> sqs = new LinkedList<>();
        private Iterator<ScanQuery> sqIterator;

        private ScanQueryProducer scanQueryProducer;
        private Iterator<ScanQuery[]> scanQueryIterator;

        ScanIteratorSupplier(String graph, ScanQueryRequest request, HgStoreWrapperEx wrapper) {
            this.graph = graph;
            this.perKeyLimit = request.getPerKeyLimit();
            this.perKeyMax=request.getPerKeyMax();
            this.skipDegree =
                    request.getSkipDegree() == 0 ? Integer.MAX_VALUE : request.getSkipDegree();
            this.wrapper = wrapper;

            if (this.perKeyLimit <= 0) {
                this.perKeyLimit = Integer.MAX_VALUE;
                log.warn("as perKeyLimit <=0 so default perKeyLimit was effective: {}", Integer.MAX_VALUE);
            }
            //init(request);
            init2(request);
        }

        private void init(ScanQueryRequest request) {
            this.sqs = Arrays.stream(request.getTable().split(","))
                    .map(table -> {
                                if (table == null) return null;
                                if (table.isEmpty()) return null;

                                List<ScanQuery> list = request.getConditionList()
                                        .stream()
                                        .map(condition -> {
                                            ScanQuery sq = ScanQuery.of();
                                            sq.graph = this.graph;
                                            sq.table = table;
                                            sq.method = request.getMethod();
                                            sq.scanType = request.getScanType();
                                            sq.query = request.getQuery().toByteArray();
                                            sq.position = request.getPosition().toByteArray();

                                            sq.keyCode = condition.getCode();
                                            sq.start = condition.getStart().toByteArray();
                                            sq.end = condition.getEnd().toByteArray();
                                            sq.prefix = condition.getPrefix().toByteArray();
                                            sq.serialNo = condition.getSerialNo();
                                            return sq;
                                        })
                                        .filter(e -> e != null)
                                        .collect(Collectors.toList());

                                if (list == null || list.isEmpty()) {
                                    ScanQuery sq = ScanQuery.of();
                                    sq.graph = this.graph;
                                    sq.table = table;
                                    sq.method = request.getMethod();
                                    sq.scanType = request.getScanType();
                                    sq.query = request.getQuery().toByteArray();
                                    sq.position = request.getPosition().toByteArray();
                                    list = Collections.singletonList(sq);
                                }
                                return list;

                            }
                    )
                    .flatMap(e -> e.stream())
                    .collect(Collectors.toList());

            this.sqIterator = this.sqs.iterator();
        }


        //@Override
        public KVPair<QueryCondition, ScanIterator> get1() {
            ScanIterator iterator = null;
            ScanQuery query = null;
            if (this.sqIterator != null && this.sqIterator.hasNext()) {
                query = this.sqIterator.next();
                iterator = getIterator(query, this.wrapper);
            } else {
                this.sqs.clear();
                this.sqIterator = null;
            }
            return new KVPair<>(query, iterator);
        }

        public Supplier<Long> getLimitSupplier() {
            return () -> Math.min(perKeyLimit, skipDegree);
        }

        /*----------- new -to add max --------------*/

        private void init2(ScanQueryRequest request) {
            List<String> tableList = Arrays.stream(request.getTable().split(","))
                    .filter(e -> e != null && !e.isEmpty())
                    .collect(Collectors.toList());

            if (tableList.isEmpty()) {
                throw new RuntimeException("table name is invalid");
            }

            String[] tables = tableList.toArray(new String[tableList.size()]);
            this.scanQueryProducer = ScanQueryProducer.requestOf(this.graph, tables, request);
            this.scanQueryIterator = this.scanQueryProducer.groupedIterator();
        }

        @Override
        public KVPair<QueryCondition, ScanIterator> get() {
            ScanIterator iterator = null;
            ScanQuery query = null;

            if (this.scanQueryIterator != null && this.scanQueryIterator.hasNext()) {
                ScanQuery[] queries = this.scanQueryIterator.next();
                query = queries[0];
                iterator = FusingScanIterator.maxOf(this.perKeyMax, new Query2Iterator((queries)));
            } else {
                this.scanQueryProducer = null;
                this.scanQueryIterator = null;
            }
            return new KVPair<>(query, iterator);
        }

        private class Query2Iterator implements Supplier<ScanIterator> {
            ScanQuery[] queries;
            int index;

            Query2Iterator(ScanQuery[] queries) {
                this.queries = queries;
            }

            @Override
            public ScanIterator get() {
                if (index + 1 > queries.length) return null;
                return getIterator(queries[index++], wrapper);
            }
        }

    }
}