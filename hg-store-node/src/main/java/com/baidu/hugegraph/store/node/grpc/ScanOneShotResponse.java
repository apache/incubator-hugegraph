package com.baidu.hugegraph.store.node.grpc;

import static com.baidu.hugegraph.store.node.grpc.ScanUtil.getIterator;
import static com.baidu.hugegraph.store.node.grpc.ScanUtil.toSq;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.grpc.common.Kv;
import com.baidu.hugegraph.store.grpc.stream.KvPageRes;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamReq;
import com.baidu.hugegraph.store.node.util.HgGrpc;
import com.baidu.hugegraph.store.node.util.HgStoreNodeUtil;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;


/**
 * @author lynn.bond@hotmail.com created on 2022/02/17
 * @version 3.6.0
 */
@Slf4j
public class ScanOneShotResponse{

    /**
     * Handle one-shot scan
     *
     * @param request
     * @param responseObserver
     */
    public static void scanOneShot(ScanStreamReq request, StreamObserver<KvPageRes> responseObserver,
                                   HgStoreWrapperEx wrapper) {
        KvPageRes.Builder resBuilder = KvPageRes.newBuilder();
        Kv.Builder kvBuilder = Kv.newBuilder();
        ScanIterator iterator = getIterator(toSq(request), wrapper);

        long limit = request.getLimit();

        if (limit <= 0) {
            responseObserver.onError(HgGrpc.toErr("limit<=0, please to invoke stream scan."));
            return;
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
            String msg = "an exception occurred during data scanning";
            responseObserver.onError(HgGrpc.toErr(Status.INTERNAL, msg, t));
        }finally {
            iterator.close();
        }

    }


}