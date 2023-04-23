package com.baidu.hugegraph.store.node.grpc.scan;

import java.util.concurrent.ThreadPoolExecutor;

import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import com.baidu.hugegraph.store.business.BusinessHandler;
import com.baidu.hugegraph.store.grpc.GraphStoreGrpc.GraphStoreImplBase;
import com.baidu.hugegraph.store.grpc.Graphpb;
import com.baidu.hugegraph.store.grpc.Graphpb.ResponseHeader;
import com.baidu.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import com.baidu.hugegraph.store.grpc.Graphpb.ScanResponse;
import com.baidu.hugegraph.store.node.grpc.HgStoreNodeService;
import com.baidu.hugegraph.store.node.grpc.HgStoreStreamImpl;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * graphpb.proto 实现类
 */
@Slf4j
@GRpcService
public class GraphStoreImpl extends GraphStoreImplBase {

    @Autowired
    private HgStoreNodeService storeService;
    @Autowired
    private HgStoreStreamImpl storeStream;
    BusinessHandler handler;

    private ResponseHeader okHeader =
            ResponseHeader.newBuilder().setError(
                                  Graphpb.Error.newBuilder().setType(Graphpb.ErrorType.OK))
                          .build();

    public BusinessHandler getHandler() {
        if (this.handler == null) {
            synchronized (this) {
                if (this.handler == null) {
                    this.handler =
                            storeService.getStoreEngine().getBusinessHandler();
                }
            }
        }
        return this.handler;
    }

    public ThreadPoolExecutor getExecutor() {
        return this.storeStream.getExecutor();
    }


    /**
     * 流式回复消息，每个消息带有seqNo
     * 客户端每消费一个消息，应答一个seqNo
     * 服务端根据客户端的seqNo决定发送几个数据包
     *
     * @param ro
     * @return
     */
    @Override
    public StreamObserver<ScanPartitionRequest> scanPartition(
            StreamObserver<ScanResponse> ro) {
        return new ScanResponseObserver(ro, getHandler(), getExecutor());
    }

}
