package com.baidu.hugegraph.store.node.grpc;


import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import com.baidu.hugegraph.store.grpc.state.HgStoreStateGrpc;
import com.baidu.hugegraph.store.grpc.state.NodeStateRes;
import com.baidu.hugegraph.store.grpc.state.ScanState;
import com.baidu.hugegraph.store.grpc.state.SubStateReq;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/3
 */
@Slf4j
@GRpcService
public class HgStoreStateService extends HgStoreStateGrpc.HgStoreStateImplBase {

    @Autowired
    HgStoreStreamImpl impl;

    @Override
    public void subState(SubStateReq request, StreamObserver<NodeStateRes> observer) {
        HgStoreStateSubject.addObserver(request.getSubId(), observer);
    }

    @Override
    public void unsubState(SubStateReq request, StreamObserver<Empty> observer) {
        HgStoreStateSubject.removeObserver(request.getSubId());
    }

    @Override
    public void getScanState(SubStateReq request, StreamObserver<ScanState> observer) {
        ScanState state = impl.getState();
        observer.onNext(state);
        observer.onCompleted();
    }
}
