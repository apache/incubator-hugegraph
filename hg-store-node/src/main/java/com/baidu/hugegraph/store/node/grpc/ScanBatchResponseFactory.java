package com.baidu.hugegraph.store.node.grpc;

import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;
import com.baidu.hugegraph.store.grpc.stream.KvStream;
import com.baidu.hugegraph.store.node.util.HgAssert;
import io.grpc.stub.StreamObserver;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

public class ScanBatchResponseFactory {
    private final static ScanBatchResponseFactory instance = new ScanBatchResponseFactory();

    public static ScanBatchResponseFactory getInstance() {
        return instance;
    }

    private Set<StreamObserver> streamObservers = new ConcurrentHashSet<>();

    public int addStreamObserver(StreamObserver observer) {
        streamObservers.add(observer);
        return streamObservers.size();
    }

    public int removeStreamObserver(StreamObserver observer) {
        streamObservers.remove(observer);
        return streamObservers.size();
    }

    /**
     * 检查是否Stream是否活跃，超时的Stream及时关闭
     */
    public void checkStreamActive() {
        streamObservers.forEach(streamObserver -> {
            ((ScanBatchResponse) streamObserver).checkActiveTimeout();
        });
    }

    public static StreamObserver of(StreamObserver<KvStream> responseObserver, HgStoreWrapperEx wrapper, ThreadPoolExecutor executor) {
        StreamObserver observer = new ScanBatchResponse(responseObserver, wrapper, executor);
        getInstance().addStreamObserver(observer);
        getInstance().checkStreamActive();
        return observer;
    }
}
