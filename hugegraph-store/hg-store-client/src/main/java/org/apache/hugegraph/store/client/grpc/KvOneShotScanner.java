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

import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgPageSize;
import org.apache.hugegraph.store.HgSeekAble;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.HgStoreClientUtil;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc.HgStoreStreamBlockingStub;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/12/1
 */
@Slf4j
@NotThreadSafe
class KvOneShotScanner implements KvCloseableIterator<Kv>, HgPageSize, HgSeekAble {
    private static final HgStoreClientConfig storeClientConfig = HgStoreClientConfig.of();
    private final HgStoreNodeSession session;
    private final HgStoreStreamBlockingStub stub;
    private final ScanStreamReq.Builder reqBuilder = ScanStreamReq.newBuilder();
    private final String table;
    private final HgOwnerKey startKey;
    private final HgOwnerKey endKey;
    private final HgOwnerKey prefix;
    private final ScanMethod scanMethod;
    private final long limit;
    private final int partition;
    private final int scanType;
    private final byte[] query;
    private final int pageSize;
    private ScanStreamReq req;
    private Iterator<Kv> iterator;
    private List<Kv> list = null;
    private boolean in = true;
    private byte[] nodePosition = HgStoreClientConst.EMPTY_BYTES;

    private KvOneShotScanner(ScanMethod scanMethod, HgStoreNodeSession session,
                             HgStoreStreamBlockingStub stub,
                             String table, HgOwnerKey prefix, HgOwnerKey startKey,
                             HgOwnerKey endKey, long limit,
                             int partition, int scanType, byte[] query) {
        this.scanMethod = scanMethod;
        this.session = session;
        this.stub = stub;
        this.table = table;
        this.startKey = toOk(startKey);
        this.endKey = toOk(endKey);
        this.prefix = toOk(prefix);
        this.partition = partition;
        this.scanType = scanType;
        this.query = query != null ? query : HgStoreClientConst.EMPTY_BYTES;
        this.limit = limit <= HgStoreClientConst.NO_LIMIT ? Integer.MAX_VALUE :
                     limit; // <=0 means no limit
        this.pageSize = storeClientConfig.getNetKvScannerPageSize();

    }

    public static KvCloseableIterator<Kv> scanAll(HgStoreNodeSession session,
                                                  HgStoreStreamBlockingStub stub,
                                                  String table, long limit, byte[] query) {
        return new KvOneShotScanner(ScanMethod.ALL, session, stub, table, null, null, null, limit,
                                    -1, HgKvStore.SCAN_ANY,
                                    query);
    }

    public static KvCloseableIterator<Kv> scanPrefix(HgStoreNodeSession session,
                                                     HgStoreStreamBlockingStub stub,
                                                     String table, HgOwnerKey prefix, long limit,
                                                     byte[] query) {
        return new KvOneShotScanner(ScanMethod.PREFIX, session, stub, table, prefix, null, null,
                                    limit,
                                    prefix.getKeyCode(), HgKvStore.SCAN_PREFIX_BEGIN, query);
    }

    public static KvCloseableIterator<Kv> scanRange(HgStoreNodeSession nodeSession,
                                                    HgStoreStreamBlockingStub stub,
                                                    String table, HgOwnerKey startKey,
                                                    HgOwnerKey endKey, long limit,
                                                    int scanType, byte[] query) {
        return new KvOneShotScanner(ScanMethod.RANGE, nodeSession, stub, table, null, startKey,
                                    endKey, limit,
                                    startKey.getKeyCode(), scanType, query);
    }

    static HgOwnerKey toOk(HgOwnerKey key) {
        return key == null ? HgStoreClientConst.EMPTY_OWNER_KEY : key;
    }

    static ByteString toBs(byte[] bytes) {
        return ByteString.copyFrom((bytes != null) ? bytes : HgStoreClientConst.EMPTY_BYTES);
    }

    private Header getHeader(HgStoreNodeSession nodeSession) {
        return Header.newBuilder().setGraph(nodeSession.getGraphName()).build();
    }

    private void createReq() {
        this.req = this.reqBuilder
                .setHeader(this.getHeader(this.session))
                .setMethod(this.scanMethod)
                .setTable(this.table)
                .setStart(toBs(this.startKey.getKey()))
                .setEnd(toBs(this.endKey.getKey()))
                .setLimit(this.limit)
                .setPrefix(toBs(this.prefix.getKey()))
                .setCode(this.partition)
                .setScanType(this.scanType)
                .setQuery(toBs(this.query))
                .setPageSize(pageSize)
                .setPosition(toBs(this.nodePosition))
                .build();
    }

    private void init() {

        if (this.iterator == null) {
            this.createReq();
            this.list = this.stub.scanOneShot(this.req).getDataList();
            this.iterator = this.list.iterator();
        }

    }

    @Override
    public boolean hasNext() {
        if (!this.in) {
            return false;
        }
        if (this.iterator == null) {
            this.init();
        }
        return this.iterator.hasNext();
    }

    @Override
    public Kv next() {
        if (this.iterator == null) {
            this.init();
        }
        return this.iterator.next();
    }

    @Override
    public long getPageSize() {
        return this.limit;
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
        //TODO: implements
    }
}