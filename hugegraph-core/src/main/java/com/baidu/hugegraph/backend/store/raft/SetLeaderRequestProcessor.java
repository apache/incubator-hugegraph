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

package com.baidu.hugegraph.backend.store.raft;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.SetLeaderRequest;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.SetLeaderResponse;
import com.baidu.hugegraph.util.Log;
import com.google.protobuf.Message;

public class SetLeaderRequestProcessor
       extends RpcRequestProcessor<SetLeaderRequest> {

    private static final Logger LOG = Log.logger(SetLeaderRequestProcessor.class);

    private final RaftSharedContext context;

    public SetLeaderRequestProcessor(RaftSharedContext context) {
        super(null, SetLeaderResponse.newBuilder().setStatus(true).build());
        this.context = context;
    }

    @Override
    public Message processRequest(SetLeaderRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing SetLeaderRequest {}", request.getClass());
        RaftNodeManager nodeManager = this.context.raftNodeManager();
        try {
            String endpoint = request.getEndpoint();
            nodeManager.setLeader(endpoint);
            return SetLeaderResponse.newBuilder().setStatus(true).build();
        } catch (Throwable e) {
            SetLeaderResponse.Builder builder;
            builder = SetLeaderResponse.newBuilder().setStatus(false);
            if (e.getMessage() != null) {
                builder.setMessage(e.getMessage());
            }
            return builder.build();
        }
    }

    @Override
    public String interest() {
        return SetLeaderRequest.class.getName();
    }
}
