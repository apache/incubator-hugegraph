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

package org.apache.hugegraph.store.node.grpc;

import static org.apache.hugegraph.store.node.grpc.ScanUtil.getParallelIterator;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.buffer.ByteBufferAllocator;
import org.apache.hugegraph.store.buffer.KVByteBuffer;
import org.apache.hugegraph.store.grpc.stream.KvStream;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.node.util.HgGrpc;
import org.apache.hugegraph.store.node.util.PropertyUtil;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * 批量查询处理器，批量查询数据，流式返回数据。
 * 1、服务端流式发送数据给客户端
 * 2、客户端每消费一批次数据，返回批次号给服务端
 * 3、服务端根据批次号决定发送多少数据，保证传送数据的不间断，
 */
@Slf4j
public class ScanBatchResponse implements StreamObserver<ScanStreamBatchReq> {

    static ByteBufferAllocator bfAllocator =
            new ByteBufferAllocator(ParallelScanIterator.maxBodySize * 3 / 2, 1000);
    static ByteBufferAllocator alloc =
            new ByteBufferAllocator(ParallelScanIterator.maxBodySize * 3 / 2, 1000);
    private final int maxInFlightCount = PropertyUtil.getInt("app.scan.stream.inflight", 16);
    private final int activeTimeout = PropertyUtil.getInt("app.scan.stream.timeout", 60); //单位秒
    private final StreamObserver<KvStream> sender;
    private final HgStoreWrapperEx wrapper;
    private final ThreadPoolExecutor executor;
    private final Object stateLock = new Object();
    private final Lock iteratorLock = new ReentrantLock();
    // 当前正在遍历的迭代器
    private ScanIterator iterator;
    // 下一次发送的序号
    private volatile int seqNo;
    // Client已消费的序号
    private volatile int clientSeqNo;
    // 已经发送的条目数
    private volatile long count;
    // 客户端要求返回的最大条目数
    private volatile long limit;
    private ScanQueryRequest query;
    // 上次读取数据时间
    private long activeTime;
    private volatile State state;

    public ScanBatchResponse(StreamObserver<KvStream> response, HgStoreWrapperEx wrapper,
                             ThreadPoolExecutor executor) {
        this.sender = response;
        this.wrapper = wrapper;
        this.executor = executor;
        this.iterator = null;
        this.seqNo = 1;
        this.state = State.IDLE;
        this.activeTime = System.currentTimeMillis();
    }

    /**
     * 接收客户端发送的消息
     * 服务端另起线程处理消息，不阻塞网络
     *
     * @param request
     */
    @Override
    public void onNext(ScanStreamBatchReq request) {
        switch (request.getQueryCase()) {
            case QUERY_REQUEST: // 查询条件
                executor.execute(() -> {
                    startQuery(request.getHeader().getGraph(), request.getQueryRequest());
                });
                break;
            case RECEIPT_REQUEST:   // 消息异步应答
                this.clientSeqNo = request.getReceiptRequest().getTimes();
                if (seqNo - clientSeqNo < maxInFlightCount) {
                    synchronized (stateLock) {
                        if (state == State.IDLE) {
                            state = State.DOING;
                            executor.execute(() -> {
                                sendEntries();
                            });
                        } else if (state == State.DONE) {
                            sendNoDataEntries();
                        }
                    }
                }
                break;
            case CANCEL_REQUEST:    // 关闭流
                closeQuery();
                break;
            default:
                sender.onError(
                        HgGrpc.toErr("Unsupported sub-request: [ " + request + " ]"));
        }
    }

    @Override
    public void onError(Throwable t) {
        log.error("onError ", t);
        closeQuery();
    }

    @Override
    public void onCompleted() {
        closeQuery();
    }

    /**
     * 生成迭代器
     *
     * @param request
     */
    private void startQuery(String graphName, ScanQueryRequest request) {
        this.query = request;
        this.limit = request.getLimit();
        this.count = 0;
        this.iterator = getParallelIterator(graphName, request, this.wrapper, executor);
        synchronized (stateLock) {
            if (state == State.IDLE) {
                state = State.DOING;
                executor.execute(() -> {
                    sendEntries();
                });
            }
        }
    }

    /**
     * 生成迭代器
     */
    private void closeQuery() {
        setStateDone();
        try {
            closeIter();
            this.sender.onCompleted();
        } catch (Exception e) {
            log.error("exception ", e);
        }
        int active = ScanBatchResponseFactory.getInstance().removeStreamObserver(this);
        log.info("ScanBatchResponse closeQuery, active count is {}", active);
    }

    private void closeIter() {
        try {
            if (this.iterator != null) {
                this.iterator.close();
                this.iterator = null;
            }
        } catch (Exception e) {

        }
    }

    /**
     * 发送数据
     */
    private void sendEntries() {
        if (state == State.DONE || iterator == null) {
            setStateIdle();
            return;
        }
        iteratorLock.lock();
        try {
            if (state == State.DONE || iterator == null) {
                setStateIdle();
                return;
            }
            KvStream.Builder dataBuilder = KvStream.newBuilder().setVersion(1);
            while (state != State.DONE && iterator.hasNext()
                   && (seqNo - clientSeqNo < maxInFlightCount)
                   && this.count < limit) {
                KVByteBuffer buffer = new KVByteBuffer(alloc.get());
                List<ParallelScanIterator.KV> dataList = iterator.next();
                dataList.forEach(kv -> {
                    kv.write(buffer);
                    this.count++;
                });
                dataBuilder.setStream(buffer.flip().getBuffer());
                dataBuilder.setSeqNo(seqNo++);
                dataBuilder.complete(e -> alloc.release(buffer.getBuffer()));
                this.sender.onNext(dataBuilder.build());
                this.activeTime = System.currentTimeMillis();
            }
            if (!iterator.hasNext() || this.count >= limit || state == State.DONE) {
                closeIter();
                this.sender.onNext(KvStream.newBuilder().setOver(true).build());
                setStateDone();
            } else {
                setStateIdle();
            }
        } catch (Throwable e) {
            if (this.state != State.DONE) {
                log.error(" send data exception: ", e);
                setStateIdle();
                if (this.sender != null) {
                    try {
                        this.sender.onError(e);
                    } catch (Exception ex) {

                    }
                }
            }
        } finally {
            iteratorLock.unlock();
        }
    }

    private void sendNoDataEntries() {
        try {
            this.sender.onNext(KvStream.newBuilder().setOver(true).build());
        } catch (Exception e) {
        }
    }

    private State setStateDone() {
        synchronized (this.stateLock) {
            this.state = State.DONE;
        }
        return state;
    }

    private State setStateIdle() {
        synchronized (this.stateLock) {
            if (this.state != State.DONE) {
                this.state = State.IDLE;
            }
        }
        return state;
    }

    /**
     * 检查是否活跃，超过一定时间客户端没有请求数据，认为已经不活跃，关闭连接释放资源
     */
    public void checkActiveTimeout() {
        if ((System.currentTimeMillis() - activeTime) > activeTimeout * 1000L) {
            log.warn("The stream is not closed, and the timeout is forced to close");
            closeQuery();
        }
    }

    /**
     * 任务状态
     */
    private enum State {
        IDLE,
        DOING,
        DONE,
        ERROR
    }
}
