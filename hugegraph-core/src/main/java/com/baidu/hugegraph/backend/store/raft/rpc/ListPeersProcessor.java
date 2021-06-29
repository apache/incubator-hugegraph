/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.raft.rpc;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.baidu.hugegraph.backend.store.raft.RaftGroupManager;
import com.baidu.hugegraph.backend.store.raft.RaftSharedContext;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.CommonResponse;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.ListPeersRequest;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.ListPeersResponse;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

public class ListPeersProcessor
       extends RpcRequestProcessor<ListPeersRequest> {

    private static final Logger LOG = Log.logger(ListPeersProcessor.class);

    private final RaftSharedContext context;

    public ListPeersProcessor(RaftSharedContext context) {
        super(null, null);
        this.context = context;
    }

    @Override
    public Message processRequest(ListPeersRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing ListPeersRequest {}", request.getClass());
        RaftGroupManager nodeManager = this.context.raftNodeManager();
        try {
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(true)
                                                  .build();
            return ListPeersResponse.newBuilder()
                                    .setCommon(common)
                                    .addAllEndpoints(nodeManager.listPeers())
                                    .build();
        } catch (Throwable e) {
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(false)
                                                  .setMessage(e.toString())
                                                  .build();
            return ListPeersResponse.newBuilder()
                                    .setCommon(common)
                                    .addAllEndpoints(ImmutableList.of())
                                    .build();
        }
    }

    @Override
    public String interest() {
        return ListPeersRequest.class.getName();
    }
}
