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
 * Batch Query Processor, query data in batches, and return data.
 * 1. Server streaming data to the client
 * 2. The Client Consumes A Batch of Data Per Consumption, returns the batch number to the server
 * 3. How Much Data DOES The Server Decide According to the Batch Number to ensure the continuous
 * transmission data,
 */
@Slf4j
public class ScanBatchResponse implements StreamObserver<ScanStreamBatchReq> {

    static ByteBufferAllocator bfAllocator =
            new ByteBufferAllocator(ParallelScanIterator.maxBodySize * 3 / 2, 1000);
    static ByteBufferAllocator alloc =
            new ByteBufferAllocator(ParallelScanIterator.maxBodySize * 3 / 2, 1000);
    private final int maxInFlightCount = PropertyUtil.getInt("app.scan.stream.inflight", 16);
    private final int activeTimeout = PropertyUtil.getInt("app.scan.stream.timeout", 60);
    // Unit seconds
    private final StreamObserver<KvStream> sender;
    private final HgStoreWrapperEx wrapper;
    private final ThreadPoolExecutor executor;
    private final Object stateLock = new Object();
    private final Lock iteratorLock = new ReentrantLock();
    // The iterators currently traversed
    private ScanIterator iterator;
    // The serial number of the next time
    private volatile int seqNo;
    // CLIENT's serial number that has been consumed
    private volatile int clientSeqNo;
    // Number of entries that have been sent
    private volatile long count;
    // The maximum number of entries required by the client requirement
    private volatile long limit;
    private ScanQueryRequest query;
    // Last reading data time
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
     * Receive messages sent by the client
     * Services on the Server to Process Messages, not blocking the network
     *
     * @param request
     */
    @Override
    public void onNext(ScanStreamBatchReq request) {
        switch (request.getQueryCase()) {
            case QUERY_REQUEST: // Query conditions
                executor.execute(() -> {
                    startQuery(request.getHeader().getGraph(), request.getQueryRequest());
                });
                break;
            case RECEIPT_REQUEST:   // Message response asynchronous
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
            case CANCEL_REQUEST:    // Close flowing
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
     * Generate iterator
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
     * Generate iterator
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
     * send data
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
     * Check WHETHER It is Active, for more than a certain period of time, the client does not
     * have request data, and it is believed that it is not active and close the connection to
     * release the resource
     */
    public void checkActiveTimeout() {
        if ((System.currentTimeMillis() - activeTime) > activeTimeout * 1000L) {
            log.warn("The stream is not closed, and the timeout is forced to close");
            closeQuery();
        }
    }

    /**
     * Task status
     */
    private enum State {
        IDLE,
        DOING,
        DONE,
        ERROR
    }
}
