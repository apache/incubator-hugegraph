package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.watch.PDWatchSubject;
import com.baidu.hugegraph.pd.grpc.watch.HgPdWatchGrpc;
import com.baidu.hugegraph.pd.grpc.watch.WatchRequest;
import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */
@Slf4j
@GRpcService
public class PDWatchService extends HgPdWatchGrpc.HgPdWatchImplBase {

    @Override
    public StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> responseObserver) {
        return PDWatchSubject.addObserver(responseObserver);
    }
}
