package com.baidu.hugegraph.store.client.grpc;

import javax.annotation.concurrent.ThreadSafe;

import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgScanQuery;
import com.baidu.hugegraph.store.client.HgStoreNodeSession;
import com.baidu.hugegraph.store.grpc.common.Kv;
import com.baidu.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import com.baidu.hugegraph.store.grpc.stream.HgStoreStreamGrpc.HgStoreStreamBlockingStub;
import com.baidu.hugegraph.store.grpc.stream.HgStoreStreamGrpc.HgStoreStreamStub;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.AbstractBlockingStub;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/19
 * @version 1.1.1 added synchronized in getChannel.
 */
@Slf4j
@ThreadSafe
public class GrpcStoreStreamClient extends AbstractGrpcClient {

    public HgStoreStreamStub getStub(HgStoreNodeSession nodeSession) {
        return (HgStoreStreamStub) getAsyncStub(nodeSession.getStoreNode().getAddress());
    }

    @Override
    public AbstractAsyncStub getAsyncStub(ManagedChannel channel){
        return HgStoreStreamGrpc.newStub(channel);
    }

    private HgStoreStreamBlockingStub getBlockingStub(HgStoreNodeSession nodeSession) {
        return (HgStoreStreamBlockingStub) getBlockingStub(nodeSession.getStoreNode().getAddress());
    }

    @Override
    public AbstractBlockingStub getBlockingStub(ManagedChannel channel) {
        return  HgStoreStreamGrpc.newBlockingStub(channel);
    }

    KvCloseableIterator<Kv> doScanOneShot(HgStoreNodeSession nodeSession, String table, long limit, byte[] query) {
        return KvOneShotScanner.scanAll(nodeSession
                , this.getBlockingStub(nodeSession)
                , table
                , limit
                , query
        );
    }

    KvCloseableIterator<Kv> doScanOneShot(HgStoreNodeSession nodeSession, String table, long limit) {
        return KvOneShotScanner.scanAll(nodeSession
                , this.getBlockingStub(nodeSession)
                , table
                , limit
                , null
        );
    }

    KvCloseableIterator<Kv> doScanOneShot(HgStoreNodeSession nodeSession, String table, HgOwnerKey prefix, long limit) {
        return KvOneShotScanner.scanPrefix(nodeSession
                , this.getBlockingStub(nodeSession)
                , table
                , prefix
                , limit
                , null
        );
    }

    KvCloseableIterator<Kv> doScanOneShot(HgStoreNodeSession nodeSession, String table, HgOwnerKey prefix, long limit,
                                          byte[] query) {
        return KvOneShotScanner.scanPrefix(nodeSession
                , this.getBlockingStub(nodeSession)
                , table
                , prefix
                , limit
                , query
        );
    }

    KvCloseableIterator<Kv> doScanOneShot(HgStoreNodeSession nodeSession, String table, HgOwnerKey startKey,
                                          HgOwnerKey endKey
            , long limit
            , int scanType
            , byte[] query) {

        return KvOneShotScanner.scanRange(nodeSession
                , this.getBlockingStub(nodeSession)
                , table
                , startKey
                , endKey
                , limit
                , scanType
                , query
        );
    }


    KvCloseableIterator<Kv> doScan(HgStoreNodeSession nodeSession
            , String table
            , long limit
            , byte[] query) {

        return KvPageScanner.scanAll(nodeSession
                , this.getStub(nodeSession)
                , table
                , limit
                , query
        );
    }

    KvCloseableIterator<Kv> doScan(HgStoreNodeSession nodeSession
            , String table
            , long limit) {

        return KvPageScanner.scanAll(nodeSession
                , this.getStub(nodeSession)
                , table
                , limit
                , null
        );
    }

    KvCloseableIterator<Kv> doScan(HgStoreNodeSession nodeSession
            , String table
            , HgOwnerKey prefix
            , long limit) {

        return KvPageScanner.scanPrefix(nodeSession
                , this.getStub(nodeSession)
                , table
                , prefix
                , limit
                , null
        );
    }

    KvCloseableIterator<Kv> doScan(HgStoreNodeSession nodeSession
            , String table
            , HgOwnerKey prefix
            , long limit
            , byte[] query) {

        return KvPageScanner.scanPrefix(nodeSession
                , this.getStub(nodeSession)
                , table
                , prefix
                , limit
                , query
        );
    }

    KvCloseableIterator<Kv> doScan(HgStoreNodeSession nodeSession
            , String table
            , HgOwnerKey startKey
            , HgOwnerKey endKey
            , long limit
            , int scanType
            , byte[] query) {

        return KvPageScanner.scanRange(nodeSession
                , this.getStub(nodeSession)
                , table
                , startKey
                , endKey
                , limit
                , scanType
                , query
        );
    }

    KvCloseableIterator<Kv> doBatchScan(HgStoreNodeSession nodeSession, HgScanQuery scanQuery) {
        return KvBatchScanner5.scan(nodeSession, this.getStub(nodeSession), scanQuery);
    }


    // 返回多个小的迭代器，允许上层并行处理
    KvCloseableIterator<HgKvIterator<HgKvEntry>> doBatchScan3(HgStoreNodeSession nodeSession,
                                                              HgScanQuery scanQuery, KvCloseableIterator iterator) {
        KvBatchScanner.scan(this.getStub(nodeSession), nodeSession.getGraphName(), scanQuery, iterator);
        return iterator;
    }

    KvCloseableIterator<Kv> doBatchScanOneShot(HgStoreNodeSession nodeSession, HgScanQuery scanQuery) {
        return KvBatchOneShotScanner.scan(nodeSession, this.getBlockingStub(nodeSession), scanQuery);
    }

}
