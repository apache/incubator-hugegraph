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

import static com.baidu.hugegraph.backend.store.raft.RaftSharedContext.WAIT_RPC_TIMEOUT;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.util.Endpoint;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.raft.RaftClosure;
import com.baidu.hugegraph.backend.store.raft.RaftNode;
import com.baidu.hugegraph.backend.store.raft.StoreClosure;
import com.baidu.hugegraph.backend.store.raft.StoreCommand;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.CommonResponse;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandRequest;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandResponse;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;

public class RpcForwarder {

    private static final Logger LOG = Log.logger(RpcForwarder.class);

    private final PeerId nodeId;
    private final RaftClientService rpcClient;

    public RpcForwarder(RaftNode node) {
        this.nodeId = node.node().getNodeId().getPeerId();
        this.rpcClient = ((NodeImpl) node.node()).getRpcService();
        E.checkNotNull(this.rpcClient, "rpc client");
    }

    public void forwardToLeader(PeerId leaderId, StoreCommand command,
                                StoreClosure closure) {
        E.checkNotNull(leaderId, "leader id");
        E.checkState(!leaderId.equals(this.nodeId),
                     "Invalid state: current node is the leader, there is " +
                             "no need to forward the request");
        LOG.debug("The node {} forward request to leader {}",
                  this.nodeId, leaderId);

        StoreCommandRequest.Builder builder = StoreCommandRequest.newBuilder();
        builder.setType(command.type());
        builder.setAction(command.action());
        builder.setData(ZeroByteStringHelper.wrap(command.data()));
        StoreCommandRequest request = builder.build();

        RpcResponseClosure<StoreCommandResponse> responseClosure;
        responseClosure = new RpcResponseClosure<StoreCommandResponse>() {
            @Override
            public void setResponse(StoreCommandResponse resp) {
                if (resp.getStatus()) {
                    LOG.debug("StoreCommandResponse status ok");
                    closure.complete(Status.OK(), () -> null);
                } else {
                    LOG.debug("StoreCommandResponse status error");
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e;
                    e = new BackendException(
                            "Current node isn't leader, leader is [%s], " +
                            "failed to forward request to leader: %s",
                            leaderId, resp.getMessage());
                    closure.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                closure.run(status);
            }
        };
        this.waitRpc(leaderId.getEndpoint(), request, responseClosure);
    }

    public <T extends Message> RaftClosure<T> forwardToLeader(PeerId leaderId,
                                                              Message request) {
        E.checkNotNull(leaderId, "leader id");
        E.checkState(!leaderId.equals(this.nodeId),
                     "Invalid state: current node is the leader, there is " +
                     "no need to forward the request");
        LOG.debug("The node {} forward request to leader {}",
                  this.nodeId, leaderId);

        RaftClosure<T> future = new RaftClosure<>();
        RpcResponseClosure<T> responseClosure;
        responseClosure = new RpcResponseClosure<T>() {
            @Override
            public void setResponse(T response) {
                FieldDescriptor fd = response.getDescriptorForType()
                                             .findFieldByName("common");
                Object object = response.getField(fd);
                E.checkState(object instanceof CommonResponse,
                             "The common field must be instance of " +
                             "CommonResponse, actual is '%s'",
                             object != null ? object.getClass() : null);
                CommonResponse commonResponse = (CommonResponse) object;
                if (commonResponse.getStatus()) {
                    future.complete(Status.OK(), () -> response);
                } else {
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e;
                    e = new BackendException(
                            "Current node isn't leader, leader is [%s], " +
                            "failed to forward request to leader: %s",
                            leaderId, commonResponse.getMessage());
                    future.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                future.run(status);
            }
        };
        this.waitRpc(leaderId.getEndpoint(), request, responseClosure);
        return future;
    }

    private <T extends Message> void waitRpc(Endpoint endpoint, Message request,
                                             RpcResponseClosure<T> done) {
        E.checkNotNull(endpoint, "leader endpoint");
        try {
            this.rpcClient.invokeWithDone(endpoint, request, done,
                                          WAIT_RPC_TIMEOUT).get();
        } catch (InterruptedException e) {
            throw new BackendException("Invoke rpc request was interrupted, " +
                                       "please try again later", e);
        } catch (ExecutionException e) {
            throw new BackendException("Failed to invoke rpc request", e);
        }
    }
}
