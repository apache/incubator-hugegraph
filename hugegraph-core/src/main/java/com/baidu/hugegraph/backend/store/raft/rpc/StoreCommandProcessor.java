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
import com.baidu.hugegraph.backend.store.raft.RaftNode;
import com.baidu.hugegraph.backend.store.raft.RaftSharedContext;
import com.baidu.hugegraph.backend.store.raft.RaftStoreClosure;
import com.baidu.hugegraph.backend.store.raft.StoreCommand;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandRequest;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandResponse;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import com.baidu.hugegraph.util.Log;
import com.google.protobuf.Message;

public class StoreCommandProcessor
       extends RpcRequestProcessor<StoreCommandRequest> {

    private static final Logger LOG = Log.logger(
                                      StoreCommandProcessor.class);

    private final RaftSharedContext context;

    public StoreCommandProcessor(RaftSharedContext context) {
        super(null, null);
        this.context = context;
    }

    @Override
    public Message processRequest(StoreCommandRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing StoreCommandRequest");
        RaftNode node = this.context.node();
        try {
            StoreCommand command = this.parseStoreCommand(request);
            RaftStoreClosure closure = new RaftStoreClosure(command);
            node.submitAndWait(command, closure);
            return StoreCommandResponse.newBuilder().setStatus(true).build();
        } catch (Throwable e) {
            StoreCommandResponse.Builder builder;
            builder = StoreCommandResponse.newBuilder().setStatus(false);
            if (e.getMessage() != null) {
                builder.setMessage(e.getMessage());
            }
            return builder.build();
        }
    }

    @Override
    public String interest() {
        return StoreCommandRequest.class.getName();
    }

    private StoreCommand parseStoreCommand(StoreCommandRequest request) {
        StoreType type = request.getType();
        StoreAction action = request.getAction();
        byte[] data = request.getData().toByteArray();
        return new StoreCommand(type, action, data, true);
    }
}
