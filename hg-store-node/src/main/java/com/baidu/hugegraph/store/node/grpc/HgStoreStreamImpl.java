package com.baidu.hugegraph.store.node.grpc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import com.baidu.hugegraph.store.grpc.state.ScanState;
import com.baidu.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import com.baidu.hugegraph.store.grpc.stream.KvPageRes;
import com.baidu.hugegraph.store.grpc.stream.KvStream;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamReq;
import com.baidu.hugegraph.store.node.AppConfig;
import com.baidu.hugegraph.store.node.util.HgExecutorUtil;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/19
 */
@Slf4j
@GRpcService
public class HgStoreStreamImpl extends HgStoreStreamGrpc.HgStoreStreamImplBase {

    @Autowired
    private HgStoreNodeService storeService;
    @Autowired
    private AppConfig appConfig;
    private HgStoreWrapperEx wrapper;
    private ThreadPoolExecutor executor;

    private HgStoreWrapperEx getWrapper() {
        if (this.wrapper == null) {
            synchronized (this) {
                if (this.wrapper == null) {
                    this.wrapper = new HgStoreWrapperEx(storeService.getStoreEngine().getBusinessHandler());
                }
            }
        }
        return this.wrapper;
    }

    public ThreadPoolExecutor getRealExecutor() {
        return executor;
    }

    public ThreadPoolExecutor getExecutor() {
        if (this.executor == null) {
            synchronized (this) {
                if (this.executor == null) {
                    AppConfig.ThreadPoolScan scan = this.appConfig.getThreadPoolScan();
                    this.executor = HgExecutorUtil.createExecutor("hg-scan", scan.getCore(), scan.getMax(),
                                                                  scan.getQueue());
                }
            }
        }
        return this.executor;
    }

    public ScanState getState() {
        ThreadPoolExecutor ex = getExecutor();
        ScanState.Builder builder = ScanState.newBuilder();
        BlockingQueue<Runnable> queue = ex.getQueue();
        ScanState state = builder.setActiveCount(ex.getActiveCount()).setTaskCount(ex.getTaskCount())
                                 .setCompletedTaskCount(ex.getCompletedTaskCount())
                                 .setMaximumPoolSize(ex.getMaximumPoolSize())
                                 .setLargestPoolSize(ex.getLargestPoolSize()).setPoolSize(ex.getPoolSize())
                                 .setAddress(appConfig.getStoreServerAddress())
                                 .setQueueSize(queue.size()).setQueueRemainingCapacity(queue.remainingCapacity())
                                 .build();
        return state;
    }

    @Override
    public StreamObserver<ScanStreamReq> scan(StreamObserver<KvPageRes> response) {
        return ScanStreamResponse.of(response, getWrapper(), getExecutor(), appConfig);
    }

    @Override
    public void scanOneShot(ScanStreamReq request, StreamObserver<KvPageRes> response) {
        ScanOneShotResponse.scanOneShot(request, response, getWrapper());
    }

    @Override
    public StreamObserver<ScanStreamBatchReq> scanBatch(StreamObserver<KvPageRes> response) {
        return ScanBatchResponse3.of(response, getWrapper(), getExecutor());
    }

    @Override
    public StreamObserver<ScanStreamBatchReq> scanBatch2(StreamObserver<KvStream> response) {
        return ScanBatchResponseFactory.of(response, getWrapper(), getExecutor());
    }

    @Override
    public void scanBatchOneShot(ScanStreamBatchReq request, StreamObserver<KvPageRes> response) {
        ScanBatchOneShotResponse.scanOneShot(request, response, getWrapper());
    }
}
