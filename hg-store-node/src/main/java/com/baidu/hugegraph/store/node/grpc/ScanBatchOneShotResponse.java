package com.baidu.hugegraph.store.node.grpc;

import static com.baidu.hugegraph.store.node.grpc.ScanUtil.getIterator;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.grpc.common.Kv;
import com.baidu.hugegraph.store.grpc.stream.KvPageRes;
import com.baidu.hugegraph.store.grpc.stream.ScanQueryRequest;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import com.baidu.hugegraph.store.node.util.HgGrpc;
import com.baidu.hugegraph.store.node.util.HgStoreNodeUtil;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;


/**
 * @author lynn.bond@hotmail.com created on 2022/04/08
 * @version 0.1.0
 */
@Slf4j
public class ScanBatchOneShotResponse {

    /**
     * Handle one-shot batch scan
     *
     * @param request
     * @param responseObserver
     */
    public static void scanOneShot(ScanStreamBatchReq request,
                                   StreamObserver<KvPageRes> responseObserver,
                                   HgStoreWrapperEx wrapper) {

        String graph = request.getHeader().getGraph();
        ScanQueryRequest queryRequest = request.getQueryRequest();
        ScanIterator iterator = getIterator(graph, queryRequest, wrapper);

        KvPageRes.Builder resBuilder = KvPageRes.newBuilder();
        Kv.Builder kvBuilder = Kv.newBuilder();

        long limit = queryRequest.getLimit();

        if (limit <= 0) {
            limit = Integer.MAX_VALUE;
            log.warn("As limit is less than or equals 0, default limit was effective:[ {} ]", Integer.MAX_VALUE);
        }

        int count = 0;

        try {
            while (iterator.hasNext()) {

                if (++count > limit) {
                    break;
                }

                RocksDBSession.BackendColumn col = iterator.next();

                resBuilder.addData(kvBuilder
                        .setKey(ByteString.copyFrom(col.name))
                        .setValue(ByteString.copyFrom(col.value))
                        .setCode(HgStoreNodeUtil.toInt(iterator.position())) //position == partition-id.
                );

            }

            responseObserver.onNext(resBuilder.build());
            responseObserver.onCompleted();

        } catch (Throwable t) {
            String msg = "Failed to do oneshot batch scan, scanning was interrupted, cause by:";
            responseObserver.onError(
                    HgGrpc.toErr(Status.Code.INTERNAL, msg, t));
        } finally {
            iterator.close();
        }

    }


}