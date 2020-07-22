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
import com.baidu.hugegraph.backend.store.raft.RaftRequests.StoreCommandRequest;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.StoreCommandResponse;
import com.baidu.hugegraph.util.Log;
import com.google.protobuf.Message;

public class StoreCommandRequestProcessor
       extends RpcRequestProcessor<StoreCommandRequest> {

    private static final Logger LOG = Log.logger(StoreCommandRequestProcessor.class);

    private final RaftSharedContext context;

    public StoreCommandRequestProcessor(RaftSharedContext context) {
        super(null, StoreCommandResponse.newBuilder().setStatus(true).build());
        this.context = context;
    }

    @Override
    public Message processRequest(StoreCommandRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing StoreCommandRequest");
        String group = request.getGroupId();
        RaftNode node = this.context.node(group);
        if (node == null) {
            String message = "No matched raft node with group: " + group;
            return StoreCommandResponse.newBuilder()
                                       .setStatus(false)
                                       .setMessage(message)
                                       .build();
        }
        try {
            StoreCommand command = this.parseStoreCommand(request);
            StoreClosure closure = new StoreClosure(command);
            node.submitCommand(command, closure);
            return StoreCommandResponse.newBuilder().setStatus(true).build();
        } catch (Throwable e) {
            return StoreCommandResponse.newBuilder()
                                       .setStatus(false)
                                       .setMessage(e.getMessage())
                                       .build();
        }
    }

    @Override
    public String interest() {
        return StoreCommandRequest.class.getName();
    }

    private StoreCommand parseStoreCommand(StoreCommandRequest request) {
        byte actionByte = (byte) request.getAction().getNumber();
        StoreAction action = StoreAction.fromCode(actionByte);
        byte[] data = request.getData().toByteArray();
        return new StoreCommand(action, data);
    }
}
