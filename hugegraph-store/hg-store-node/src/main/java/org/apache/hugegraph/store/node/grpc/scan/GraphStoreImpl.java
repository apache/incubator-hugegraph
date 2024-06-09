/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node.grpc.scan;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.grpc.GraphStoreGrpc.GraphStoreImplBase;
import org.apache.hugegraph.store.grpc.Graphpb;
import org.apache.hugegraph.store.grpc.Graphpb.ResponseHeader;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * graphpb.proto 实现类
 */
@Slf4j
@GRpcService
public class GraphStoreImpl extends GraphStoreImplBase {

    private final ResponseHeader okHeader =
            ResponseHeader.newBuilder().setError(
                                  Graphpb.Error.newBuilder().setType(Graphpb.ErrorType.OK))
                          .build();
    BusinessHandler handler;
    @Autowired
    private HgStoreNodeService storeService;
    @Autowired
    private HgStoreStreamImpl storeStream;

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
