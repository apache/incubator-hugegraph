package com.baidu.hugegraph.store.client.grpc;

import com.baidu.hugegraph.store.HgPageSize;
import com.baidu.hugegraph.store.HgScanQuery;
import com.baidu.hugegraph.store.HgSeekAble;
import com.baidu.hugegraph.store.client.HgStoreNodeSession;
import com.baidu.hugegraph.store.grpc.common.Kv;
import com.baidu.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.List;

import static com.baidu.hugegraph.store.client.grpc.KvBatchUtil.*;

/**
 * @author lynn.bond@hotmail.com created on 2022/04/08
 */
@Slf4j
@NotThreadSafe
class KvBatchOneShotScanner implements KvCloseableIterator<Kv>, HgPageSize, HgSeekAble {
    private final HgStoreNodeSession nodeSession;
    private final HgStoreStreamGrpc.HgStoreStreamBlockingStub stub;
    private final HgScanQuery scanQuery;

    private Iterator<Kv> iterator;
    private List<Kv> list = null;

    public static KvCloseableIterator scan(HgStoreNodeSession nodeSession,
                                           HgStoreStreamGrpc.HgStoreStreamBlockingStub stub,
                                           HgScanQuery scanQuery) {

        return new KvBatchOneShotScanner(nodeSession, stub, scanQuery);
    }

    private KvBatchOneShotScanner(HgStoreNodeSession nodeSession,
                                  HgStoreStreamGrpc.HgStoreStreamBlockingStub stub,
                                  HgScanQuery scanQuery) {

        this.nodeSession = nodeSession;
        this.stub = stub;
        this.scanQuery = scanQuery;
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
            this.iterator =this.createIterator();
        }
        return this.iterator.hasNext();
    }

    @Override
    public Kv next() {
        if (this.iterator == null) {
            this.iterator =this.createIterator();
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