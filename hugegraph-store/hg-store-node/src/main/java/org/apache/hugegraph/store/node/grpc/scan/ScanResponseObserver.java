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

package org.apache.hugegraph.store.node.grpc.scan;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.GraphStoreIterator;
import org.apache.hugegraph.store.grpc.Graphpb.Error;
import org.apache.hugegraph.store.grpc.Graphpb.ErrorType;
import org.apache.hugegraph.store.grpc.Graphpb.ResponseHeader;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Request;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.ScanType;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;

import com.google.protobuf.Descriptors;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScanResponseObserver<T> implements
                                     StreamObserver<ScanPartitionRequest> {

    private static final int BATCH_SIZE = 100000;
    private static final int MAX_PAGE = 8; //
    private static final Error ok = Error.newBuilder().setType(ErrorType.OK).build();
    private static final ResponseHeader okHeader =
            ResponseHeader.newBuilder().setError(ok).build();
    private final BusinessHandler handler;
    private final AtomicInteger nextSeqNo = new AtomicInteger(0);
    private final AtomicInteger cltSeqNo = new AtomicInteger(0);
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean readOver = new AtomicBoolean(false);
    private final LinkedBlockingQueue<ScanResponse> packages =
            new LinkedBlockingQueue(MAX_PAGE * 2);
    private final Descriptors.FieldDescriptor vertexField =
            ScanResponse.getDescriptor().findFieldByNumber(3);
    private final Descriptors.FieldDescriptor edgeField =
            ScanResponse.getDescriptor().findFieldByNumber(4);
    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock sendLock = new ReentrantLock();
    private StreamObserver<ScanResponse> sender;
    private ScanPartitionRequest scanReq;
    private GraphStoreIterator iter;
    private volatile long leftCount;
    private volatile Future<?> sendTask;
    private volatile Future<?> readTask;

    /*
     * 2022年11月1日
     * 1.onNext 需要进行异步处理，以防止grpc的调用阻塞
     * 2.不要读取迭代器或者发送数据不要产生线程等待
     * 3.在发送前，尽量准备好要发送的数据
     * */

    /*
     * 2022年11月2日
     * 1.读取rocksdb迭代器的线程read
     * 2.进行数据转换并发送到阻塞队列的线程offer
     * 3.从阻塞队列读取数据，并发送的线程，包括在没有读取到数据的情况下唤醒读取和发送的线程send
     * */

    public ScanResponseObserver(StreamObserver<ScanResponse> sender,
                                BusinessHandler handler,
                                ThreadPoolExecutor executor) {
        this.sender = sender;
        this.handler = handler;
        this.executor = executor;
    }

    private boolean readCondition() {
        return packages.remainingCapacity() != 0 && !readOver.get();
    }

    private boolean readTaskCondition() {
        return readCondition() && (readTask == null || readTask.isDone());
    }

    private boolean sendCondition() {
        return nextSeqNo.get() - cltSeqNo.get() < MAX_PAGE;
    }

    private boolean sendTaskCondition() {
        return sendCondition() && (sendTask == null || sendTask.isDone());
    }

    private void offer(Iterable<T> data, boolean isVertex) {
        ScanResponse.Builder builder = ScanResponse.newBuilder();
        builder.setHeader(okHeader).setSeqNo(nextSeqNo.get());
        if (isVertex) {
            builder = builder.setField(vertexField, data);
        } else {
            builder = builder.setField(edgeField, data);
        }
        ScanResponse response = builder.build();
        packages.offer(response);
        startSend();
    }

    private void startRead() {
        if (readTaskCondition()) {
            if (readLock.tryLock()) {
                if (readTaskCondition()) {
                    readTask = executor.submit(rr);
                }
                readLock.unlock();
            }
        }
    }

    private void startSend() {
        if (sendTaskCondition()) {
            if (sendLock.tryLock()) {
                if (sendTaskCondition()) {
                    sendTask = executor.submit(sr);
                }
                sendLock.unlock();
            }
        }
    }

    @Override
    public void onNext(ScanPartitionRequest scanReq) {
        if (scanReq.hasScanRequest() && !scanReq.hasReplyRequest()) {
            this.scanReq = scanReq;
            Request request = scanReq.getScanRequest();
            long rl = request.getLimit();
            leftCount = rl > 0 ? rl : Long.MAX_VALUE;
            iter = handler.scan(scanReq);
            if (!iter.hasNext()) {
                close();
                sender.onCompleted();
            } else {
                readTask = executor.submit(rr);
            }
        } else {
            cltSeqNo.getAndIncrement();
            startSend();
        }
    }

    @Override
    public void onError(Throwable t) {
        close();
        log.warn("receive client error:", t);
    }

    @Override
    public void onCompleted() {
        close();
    }

    private void close() {
        try {
            nextSeqNo.set(0);
            if (sendTask != null) {
                sendTask.cancel(true);
            }
            if (readTask != null) {
                readTask.cancel(true);
            }
            readOver.set(true);
            iter.close();
        } catch (Exception e) {
            log.warn("on Complete with error:", e);
        }
    }

    Runnable rr = new Runnable() {
        @Override
        public void run() {
            try {
                if (readCondition()) {
                    synchronized (iter) {
                        while (readCondition()) {
                            Request r = scanReq.getScanRequest();
                            ScanType t = r.getScanType();
                            boolean isVertex = t.equals(ScanType.SCAN_VERTEX);
                            ArrayList<T> data = new ArrayList<>(BATCH_SIZE);
                            int count = 0;
                            while (iter.hasNext() && leftCount > -1) {
                                count++;
                                leftCount--;
                                T next = (T) iter.next();
                                data.add(next);
                                if (count >= BATCH_SIZE) {
                                    offer(data, isVertex);
                                    // data.clear();
                                    break;
                                }
                            }
                            if (!(iter.hasNext() && leftCount > -1)) {
                                if (data.size() > 0 &&
                                    data.size() < BATCH_SIZE) {
                                    offer(data, isVertex);
                                }
                                readOver.set(true);
                                data = null;
                                //log.warn("scan complete , count: {},time: {}",
                                //         sum, System.currentTimeMillis());
                                return;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("read data with error: ", e);
                sender.onError(e);
            }
        }
    };

    Runnable sr = () -> {
        while (sendCondition()) {
            ScanResponse response;
            try {
                if (readOver.get()) {
                    if ((response = packages.poll()) == null) {
                        sender.onCompleted();
                    } else {
                        sender.onNext(response);
                        nextSeqNo.incrementAndGet();
                    }
                } else {
                    response = packages.poll(10,
                                             TimeUnit.MILLISECONDS);
                    if (response != null) {
                        sender.onNext(response);
                        nextSeqNo.incrementAndGet();
                        startRead();
                    } else {
                        break;
                    }
                }

            } catch (InterruptedException e) {
                break;
            }
        }
    };
}
