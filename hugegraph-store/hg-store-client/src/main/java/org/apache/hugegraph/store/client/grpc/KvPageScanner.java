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

package org.apache.hugegraph.store.client.grpc;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgPageSize;
import org.apache.hugegraph.store.HgSeekAble;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.util.HgBufferProxy;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.HgStoreClientUtil;
import org.apache.hugegraph.store.client.util.MetricX;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc.HgStoreStreamStub;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.grpc.stream.SelectParam;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/10/21
 *
 * @version 0.6.1 lynn.bond@hotamil.com on 2022/04/05
 */
@Slf4j
@NotThreadSafe
class KvPageScanner implements KvCloseableIterator<Kv>, HgPageSize, HgSeekAble {

    private static final HgStoreClientConfig clientConfig = HgStoreClientConfig.of();
    private static final int nextTimeout = clientConfig.getNetKvScannerHaveNextTimeout();
    private final HgStoreNodeSession session;
    private final HgStoreStreamStub stub;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final SelectParam.Builder selectBuilder = SelectParam.newBuilder();
    private final BlockingQueue<ScanStreamReq> reqQueue = new LinkedBlockingQueue<>();
    private int pageSize = clientConfig.getNetKvScannerPageSize();
    private HgBufferProxy<List<Kv>> proxy;
    private Iterator<Kv> iterator;
    private StreamObserver<ScanStreamReq> observer;
    private ScanStreamReq.Builder reqBuilder = ScanStreamReq.newBuilder();
    private boolean in = true;
    private byte[] nodePosition = HgStoreClientConst.EMPTY_BYTES;

    private KvPageScanner(ScanMethod scanMethod, HgStoreNodeSession session, HgStoreStreamStub stub,
                          String table,
                          HgOwnerKey prefix, HgOwnerKey startKey, HgOwnerKey endKey, long limit,
                          int partition,
                          int scanType, byte[] query) {
        this.session = session;
        this.stub = stub;
        this.pageSize = clientConfig.getNetKvScannerPageSize();
        this.reqBuilder.setHeader(this.getHeader(this.session))
                       .setMethod(scanMethod)
                       .setTable(table)
                       .setStart(toBs(toOk(startKey).getKey()))
                       .setEnd(toBs(toOk(endKey).getKey()))
                       .setLimit(limit <= HgStoreClientConst.NO_LIMIT ? Integer.MAX_VALUE : limit)
                       .setPrefix(toBs(toOk(prefix).getKey()))
                       .setCode(partition)
                       .setScanType(scanType)
                       .setQuery(toBs(query != null ? query : HgStoreClientConst.EMPTY_BYTES))
                       .setPageSize(pageSize)
                       .setPosition(toBs(this.nodePosition));
        this.init();
    }

    public KvPageScanner(HgStoreNodeSession session, HgStoreStreamStub stub,
                         ScanStreamReq.Builder reqBuilder) {
        this.session = session;
        this.stub = stub;
        reqBuilder.setPageSize(pageSize);
        reqBuilder.setPosition(toBs(this.nodePosition));
        this.reqBuilder = reqBuilder;
        this.init();
    }

    public static KvCloseableIterator<Kv> scanAll(HgStoreNodeSession nodeSession,
                                                  HgStoreStreamStub stub, String table,
                                                  long limit, byte[] query) {
        return new KvPageScanner(ScanMethod.ALL, nodeSession, stub, table, null, null, null, limit,
                                 -1, HgKvStore.SCAN_ANY, query);
    }

    public static KvCloseableIterator<Kv> scanPrefix(HgStoreNodeSession nodeSession,
                                                     HgStoreStreamStub stub,
                                                     String table, HgOwnerKey prefix, long limit,
                                                     byte[] query) {
        return new KvPageScanner(ScanMethod.PREFIX, nodeSession, stub, table, prefix, null, null,
                                 limit,
                                 prefix.getKeyCode(), HgKvStore.SCAN_PREFIX_BEGIN, query);
    }

    public static KvCloseableIterator<Kv> scanRange(HgStoreNodeSession nodeSession,
                                                    HgStoreStreamStub stub,
                                                    String table, HgOwnerKey startKey,
                                                    HgOwnerKey endKey, long limit,
                                                    int scanType, byte[] query) {
        return new KvPageScanner(ScanMethod.RANGE, nodeSession, stub, table, null, startKey, endKey,
                                 limit,
                                 startKey.getKeyCode(), scanType, query);
    }

    static HgOwnerKey toOk(HgOwnerKey key) {
        return key == null ? HgStoreClientConst.EMPTY_OWNER_KEY : key;
    }

    static ByteString toBs(byte[] bytes) {
        return ByteString.copyFrom((bytes != null) ? bytes : HgStoreClientConst.EMPTY_BYTES);
    }

    private ScanStreamReq createScanReq() {
        return this.reqBuilder.setPosition(toBs(this.nodePosition)).build();
    }

    private ScanStreamReq createStopReq() {
        return this.reqBuilder.setHeader(this.getHeader(this.session)).setCloseFlag(1).build();
    }

    private void init() {
        this.proxy = HgBufferProxy.of(() -> this.serverScan());
        this.observer = this.stub.scan(new ServeObserverImpl());

    }

    /*** Server Event End ***/

    private void serverScan() {
        if (this.completed.get()) {
            this.proxy.close();
            return;
        }
        if (this.proxy.isClosed()) {
            return;
        }
        this.send(this.createScanReq());
    }

    private void stopSever() {
        this.send(this.createStopReq());
    }

    private void send(ScanStreamReq req) {
        if (!this.completed.get()) {
            try {
                this.observer.onNext(req);
            } catch (IllegalStateException | IllegalArgumentException e) {

            } catch (Exception e) {
                throw e;
            }
        }
    }

    private void clientError(String msg) {
        this.observer.onError(GrpcUtil.toErr(msg));
    }

    /*** Iterator ***/
    @Override
    public boolean hasNext() {
        if (!this.in) {
            return false;
        }
        // QUESTION: After `this.iterator.hasNext()` evaluates to false,
        //           no further attempts should make to reconstruct the iterator.
        if (this.iterator != null && this.iterator.hasNext()) {
            return true;
        }
        long start = 0;
        boolean debugEnabled = log.isDebugEnabled();
        if (debugEnabled) {
            start = System.nanoTime();
        }
        List<Kv> data = this.proxy.receive(nextTimeout, (sec) -> {
            String msg = "failed to receive data from net scanning, because of timeout [ " + sec +
                         " ] sec.";
            log.error(msg);
            this.clientError(msg);
            throw new RuntimeException(msg);
        });
        if (debugEnabled) {
            MetricX.plusIteratorWait(System.nanoTime() - start);
        }
        if (data != null) {
            this.iterator = data.iterator();
        } else {
            this.iterator = Collections.emptyIterator();
        }
        return this.iterator.hasNext();
    }

    @Override
    public Kv next() {
        if (this.iterator == null && !this.hasNext()) {
            throw new NoSuchElementException();
        }
        return this.iterator.next();
    }

    @Override
    public long getPageSize() {
        return this.pageSize;
    }

    @Override
    public boolean isPageEmpty() {
        return !this.iterator.hasNext();
    }

    @Override
    public byte[] position() {
        return HgStoreClientUtil.toBytes(this.session.getStoreNode().getNodeId().longValue());
    }

    @Override
    public void seek(byte[] position) {
        if (position == null || position.length < Long.BYTES) {
            return;
        }
        byte[] nodeIdBytes = new byte[Long.BYTES];
        System.arraycopy(position, 0, nodeIdBytes, 0, Long.BYTES);
        long nodeId = this.session.getStoreNode().getNodeId().longValue();
        long pId = HgStoreClientUtil.toLong(nodeIdBytes);
        this.in = nodeId >= pId;
        if (this.in && nodeId == pId) {
            this.nodePosition = new byte[position.length - Long.BYTES];
            System.arraycopy(position, Long.BYTES, this.nodePosition, 0, this.nodePosition.length);
        } else {
            this.nodePosition = HgStoreClientConst.EMPTY_BYTES;
        }
    }

    @Override
    public void close() {
        this.stopSever();
    }

    /*** commons ***/
    private Header getHeader(HgStoreNodeSession nodeSession) {
        return Header.newBuilder().setGraph(nodeSession.getGraphName()).build();
    }

    /*** Server event Start ***/
    private class ServeObserverImpl implements StreamObserver<KvPageRes> {

        @Override
        public void onNext(KvPageRes value) {
            if (value.getOver()) {
                completed.set(true);
                observer.onCompleted();
            }
            proxy.send(value.getDataList());
            if (completed.get()) {
                proxy.close();
            }
        }

        @Override
        public void onError(Throwable t) {
            completed.set(true);
            try {
                observer.onCompleted();
            } catch (Exception e) {
                log.warn("failed to invoke requestObserver.onCompleted(), reason:", e.getMessage());
            }
            proxy.close();
            proxy.setError(t);
            log.error("failed to complete scan of session: " + session, t);
        }

        @Override
        public void onCompleted() {
            completed.set(true);
            proxy.close();
        }
    }

}

