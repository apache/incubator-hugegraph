package org.apache.hugegraph.pd.client.impl;

import java.util.concurrent.ExecutorService;

import org.apache.hugegraph.pd.client.support.PDExecutors;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamDelegatorReceiver<ReqT, RespT> implements StreamObserver<RespT> {

    private final StreamDelegator<ReqT, RespT> delegator;
    private static ExecutorService connectExecutor =
            PDExecutors.newDiscardPool("on-error", 8, 8, Integer.MAX_VALUE);
    public StreamDelegatorReceiver(StreamDelegator<ReqT, RespT> delegator) {
        this.delegator = delegator;
    }

    public void onNext(RespT res) {
        this.delegator.onNext(res);
    }

    public void onError(Throwable t) {
        connectExecutor.submit(() -> this.delegator.onError(t));
    }

    public void onCompleted() {
        this.delegator.onCompleted();
    }
}
