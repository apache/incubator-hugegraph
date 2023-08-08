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

import static org.apache.hugegraph.store.client.grpc.KvBatchUtil.EMPTY_POSITION;
import static org.apache.hugegraph.store.client.grpc.KvBatchUtil.createQueryReq;
import static org.apache.hugegraph.store.client.grpc.KvBatchUtil.getHeader;

import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.HgPageSize;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgSeekAble;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/04/08
 */
@Slf4j
@NotThreadSafe
class KvBatchOneShotScanner implements KvCloseableIterator<Kv>, HgPageSize, HgSeekAble {
    private final HgStoreNodeSession nodeSession;
    private final HgStoreStreamGrpc.HgStoreStreamBlockingStub stub;
    private final HgScanQuery scanQuery;

    private Iterator<Kv> iterator;
    private List<Kv> list = null;

    private KvBatchOneShotScanner(HgStoreNodeSession nodeSession,
                                  HgStoreStreamGrpc.HgStoreStreamBlockingStub stub,
                                  HgScanQuery scanQuery) {

        this.nodeSession = nodeSession;
        this.stub = stub;
        this.scanQuery = scanQuery;
    }

    public static KvCloseableIterator scan(HgStoreNodeSession nodeSession,
                                           HgStoreStreamGrpc.HgStoreStreamBlockingStub stub,
                                           HgScanQuery scanQuery) {

        return new KvBatchOneShotScanner(nodeSession, stub, scanQuery);
    }

    private ScanStreamBatchReq createReq() {
        return ScanStreamBatchReq.newBuilder()
                                 .setHeader(getHeader(this.nodeSession))
                                 .setQueryRequest(createQueryReq(this.scanQuery, Integer.MAX_VALUE))
                                 .build();
    }

    private Iterator<Kv> createIterator() {
        this.list = this.stub.scanBatchOneShot(this.createReq()).getDataList();
        return this.list.iterator();
    }

    /*** Iterator ***/
    @Override
    public boolean hasNext() {
        if (this.iterator == null) {
            this.iterator = this.createIterator();
        }
        return this.iterator.hasNext();
    }

    @Override
    public Kv next() {
        if (this.iterator == null) {
            this.iterator = this.createIterator();
        }
        return this.iterator.next();
    }

    @Override
    public long getPageSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isPageEmpty() {
        return !this.iterator.hasNext();
    }

    @Override
    public byte[] position() {
        //TODO: to implement
        return EMPTY_POSITION;
    }

    @Override
    public void seek(byte[] position) {
        //TODO: to implement
    }

    @Override
    public void close() {
        //Nothing to do
    }

}