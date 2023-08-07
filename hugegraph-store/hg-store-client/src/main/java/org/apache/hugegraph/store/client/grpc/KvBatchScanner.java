/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.store.client.grpc;

import static org.apache.hugegraph.store.client.grpc.KvBatchUtil.createQueryReq;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvOrderedIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.buffer.KVByteBuffer;
import org.apache.hugegraph.store.client.util.PropertyUtil;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import org.apache.hugegraph.store.grpc.stream.KvStream;
import org.apache.hugegraph.store.grpc.stream.ScanReceiptRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * 批量流式查询客户端实现类
 * <p>
 * created on 2022/07/23
 *
 * @version 3.0.0
 */
@Slf4j
@NotThreadSafe
public class KvBatchScanner implements Closeable {

    static final Supplier<HgKvIterator<HgKvEntry>> NO_DATA = () -> null;
    static int maxTaskSizePerStore = PropertyUtil.getInt("net.kv.scanner.task.size", 8);
    private final StreamObserver<ScanStreamBatchReq> sender; // 命令发送器
    private final KvBatchScannerMerger notifier; // 数据通知
    private final String graphName; // 图名
    private final HgScanQuery scanQuery;
    private final ScanReceiptRequest.Builder responseBuilder = ScanReceiptRequest.newBuilder();
    private final KvBatchReceiver receiver;
    volatile int currentSeqNo = 0;
    private volatile boolean running;

    public KvBatchScanner(
            HgStoreStreamGrpc.HgStoreStreamStub stub,
            String graphName,
            HgScanQuery scanQuery,
            KvCloseableIterator iterator) {

        this.graphName = graphName;
        this.notifier = (KvBatchScannerMerger) iterator;
        this.notifier.registerScanner(this);
        this.running = true;
        this.scanQuery = scanQuery;
        receiver =
                new KvBatchReceiver(this, scanQuery.getOrderType() == ScanOrderType.ORDER_STRICT);
        sender = stub.scanBatch2(receiver);
        sendQuery(this.scanQuery); // 发送查询请求
    }

    /**
     * 构建流式查询迭代器
     * scanQuery进行拆分，启动多个流式请求，提升store的并发性
     *
     * @param scanQuery scanQuery
     * @param handler   task handler
     * @return data merger iterator
     */
    public static KvCloseableIterator ofMerger(
            HgScanQuery scanQuery, BiFunction<HgScanQuery, KvCloseableIterator, Boolean> handler) {
        KvBatchScannerMerger merger;
        if (scanQuery.getOrderType() == ScanOrderType.ORDER_STRICT) {
            merger = new KvBatchScannerMerger.SortedScannerMerger(
                    new TaskSplitter(scanQuery, handler));
        } else {
            merger = new KvBatchScannerMerger(new TaskSplitter(scanQuery, handler));
        }
        merger.startTask();
        return merger;
    }

    public static void scan(
            HgStoreStreamGrpc.HgStoreStreamStub stub,
            String graphName,
            HgScanQuery scanQuery,
            KvCloseableIterator iterator) {
        new KvBatchScanner(stub, graphName, scanQuery, iterator);
    }

    /**
     * 发送查询请求
     *
     * @param query scan query
     */
    public void sendQuery(HgScanQuery query) {
        synchronized (this.sender) {
            if (running) {
                this.sender.onNext(
                        ScanStreamBatchReq.newBuilder()
                                          .setHeader(
                                                  Header.newBuilder().setGraph(graphName).build())
                                          .setQueryRequest(createQueryReq(query, 0))
                                          .build());
            }
        }
    }

    /**
     * 发送应答
     */
    public void sendResponse() {
        try {
            sendResponse(currentSeqNo);
        } catch (Exception e) {
            log.error("exception", e);
        }
    }

    public void sendResponse(int seqNo) {
        currentSeqNo = seqNo;
        synchronized (this.sender) {
            if (running) {
                this.sender.onNext(
                        ScanStreamBatchReq.newBuilder()
                                          .setHeader(
                                                  Header.newBuilder().setGraph(graphName).build())
                                          .setReceiptRequest(
                                                  responseBuilder.setTimes(seqNo).build())
                                          .build());
            }
        }
    }

    public void dataArrived(Supplier<HgKvIterator<HgKvEntry>> supplier) throws
                                                                        InterruptedException {
        notifier.dataArrived(this, supplier);
    }

    /**
     * 数据接收结束
     */
    public void dataComplete() {
        close();
    }

    // 流被关闭
    @Override
    public void close() {
        try {
            if (notifier.unregisterScanner(this) < 0) {
                notifier.dataArrived(this, NO_DATA); // 任务结束，唤醒队列
            }
        } catch (InterruptedException e) {
            log.error("exception ", e);
        }
        synchronized (this.sender) {
            try {
                if (running) {
                    sender.onCompleted();
                }
            } catch (Exception e) {
            }
            running = false;
        }
    }

    /**
     * 任务拆分器
     */
    static class TaskSplitter {
        final HgScanQuery scanQuery;
        final BiFunction<HgScanQuery, KvCloseableIterator, Boolean> taskHandler;
        private KvBatchScannerMerger notifier;
        private Iterator<HgOwnerKey> prefixItr;
        private int maxTaskSize = 0; // 最大并行任务数
        private int maxBatchSize = PropertyUtil.getInt("net.kv.scanner.batch.size", 1000);
        // 每批次最大点数量
        private volatile boolean finished = false;
        private volatile boolean splitting = false;
        private volatile int nextKeySerialNo = 1;

        public TaskSplitter(HgScanQuery scanQuery,
                            BiFunction<HgScanQuery, KvCloseableIterator, Boolean> handler) {
            this.scanQuery = scanQuery;
            this.taskHandler = handler;
            if (scanQuery.getScanMethod() == HgScanQuery.ScanMethod.PREFIX) {
                if (scanQuery.getPrefixItr() != null) {
                    prefixItr = scanQuery.getPrefixItr();
                } else {
                    prefixItr = scanQuery.getPrefixList().listIterator();
                }
            }
        }

        public void setNotifier(KvBatchScannerMerger notifier) {
            this.notifier = notifier;
        }

        public boolean isFinished() {
            return finished;
        }

        /**
         * 评估最大任务数
         */
        private void evaluateMaxTaskSize() {
            if (maxTaskSize == 0) { // 根据第一批次任务，得到store数量，然后计算最大任务数
                if (scanQuery.getOrderType() == ScanOrderType.ORDER_STRICT) {
                    maxTaskSize = 1; // 点排序，每台机器一个流, 所有store流结束后才能启动其他流
                } else {
                    maxTaskSize = this.notifier.getScannerCount() * maxTaskSizePerStore;
                }
                maxBatchSize = this.notifier.getScannerCount() * maxBatchSize; // 每台机器最多1000条

                /*
                 * Limit少于10000时启动一个流，节省网络带宽
                 */
                if (scanQuery.getLimit() < maxBatchSize * 30L) {
                    maxTaskSize = 1;
                }
            }
        }

        /**
         * 拆分任务，任务拆分为多个grpc请求
         */
        public void splitTask() {
            if (this.finished || this.splitting) {
                return;
            }
            synchronized (this) {
                if (this.finished) {
                    return;
                }
                this.splitting = true;
                if (scanQuery.getScanMethod() == HgScanQuery.ScanMethod.PREFIX) {
                    if (prefixItr.hasNext() &&
                        (maxTaskSize == 0 || notifier.getScannerCount() < maxTaskSize)) {
                        List<HgOwnerKey> keys = new ArrayList<>(maxBatchSize);
                        for (int i = 0; i < maxBatchSize && prefixItr.hasNext(); i++) {
                            keys.add(prefixItr.next().setSerialNo(nextKeySerialNo++));
                        }
                        taskHandler.apply(
                                HgScanQuery.prefixOf(scanQuery.getTable(), keys,
                                                     scanQuery.getOrderType()), this.notifier);
                        // 评估最大任务数
                        evaluateMaxTaskSize();
                        if (this.notifier.getScannerCount() < this.maxTaskSize) {
                            splitTask(); // 未达到最大任务数，继续拆分
                        }
                    }
                    this.finished = !prefixItr.hasNext();
                } else {
                    taskHandler.apply(scanQuery, this.notifier);
                    this.finished = true;
                }
                this.splitting = false;
            }
        }

        public synchronized void close() {
            finished = true;
        }
    }

    /**
     * 查询结果接收器
     */
    static class KvBatchReceiver implements StreamObserver<KvStream> {
        KvBatchScanner scanner;
        boolean sortByVertex;

        KvBatchReceiver(KvBatchScanner scanner, boolean sortByVertex) {
            this.scanner = scanner;
            this.sortByVertex = sortByVertex;
        }

        @Override
        public void onNext(KvStream value) {
            try {
                ByteBuffer buffer = value.getStream();
                int seqNo = value.getSeqNo();
                boolean isOver = value.getOver();
                scanner.dataArrived(
                        () -> {
                            scanner.sendResponse(seqNo);
                            if (isOver) {
                                scanner.dataComplete();
                            }
                            return new KVBytesIterator(buffer, sortByVertex, scanner);
                        });
            } catch (InterruptedException e) {
                close();
                log.error("exception ", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            log.error("exception ", t);
            close();
        }

        @Override
        public void onCompleted() {
            close();
        }

        private void close() {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    static class KVBytesIterator implements HgKvOrderedIterator<HgKvEntry> {
        private final KvBatchScanner scanner;
        KVByteBuffer buffer;
        HgKvEntry entry;
        int sn;
        boolean hasSN;

        public KVBytesIterator(ByteBuffer buffer, boolean hasNo, KvBatchScanner scanner) {
            this.buffer = new KVByteBuffer(buffer);
            this.hasSN = hasNo;
            this.scanner = scanner;
        }

        @Override
        public void close() {
            // this.scanner.close();
        }

        @Override
        public byte[] key() {
            return entry.key();
        }

        @Override
        public byte[] value() {
            return entry.value();
        }

        @Override
        public byte[] position() {
            return new byte[0];
        }

        @Override
        public void seek(byte[] position) {
            throw new RuntimeException("not implemented");
        }

        @Override
        public boolean hasNext() {
            return buffer.hasRemaining();
        }

        @Override
        public HgKvEntry next() {
            if (hasSN) {
                sn = buffer.getInt();
            }
            entry = new GrpcKvEntryImpl(buffer.getBytes(), buffer.getBytes(), 0);
            return entry;
        }

        @Override
        public long getSequence() {
            return sn;
        }

        @Override
        public int compareTo(HgKvOrderedIterator o) {
            return Long.compare(this.getSequence(), o.getSequence());
        }
    }
}
