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

package org.apache.hugegraph.backend.store.raft.rpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.ClientService;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.raft.RaftClosure;
import org.apache.hugegraph.backend.store.raft.RaftContext;
import org.apache.hugegraph.backend.store.raft.RaftStoreClosure;
import org.apache.hugegraph.backend.store.raft.StoreCommand;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.CommonResponse;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandResponse;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;

public class RpcForwarder {

    private static final Logger LOG = Log.logger(RpcForwarder.class);

    private final PeerId nodeId;
    private final ClientService rpcClient;

    public RpcForwarder(Node node) {
        this.nodeId = node.getNodeId().getPeerId();
        this.rpcClient = ((NodeImpl) node).getRpcService();
        E.checkNotNull(this.rpcClient, "rpc client");
    }

    public RpcForwarder(PeerId nodeId) {
        this.nodeId = nodeId;
        this.rpcClient = new CliClientServiceImpl();
        this.rpcClient.init(new CliOptions());
    }

    public void close() {
        this.rpcClient.shutdown();
    }

    public void forwardToLeader(PeerId leaderId, StoreCommand command,
                                RaftStoreClosure future) {
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
        builder.setShardId(command.shardId());
        StoreCommandRequest request = builder.build();

        RpcResponseClosure<StoreCommandResponse> responseClosure;
        responseClosure = new RpcResponseClosure<StoreCommandResponse>() {
            @Override
            public void setResponse(StoreCommandResponse response) {
                if (response.getStatus()) {
                    LOG.debug("StoreCommandResponse status ok");
                    future.complete(Status.OK(), () -> null);
                } else {
                    LOG.debug("StoreCommandResponse status error");
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e = new BackendException(
                                         "Current node isn't leader, leader " +
                                         "is [%s], failed to forward request " +
                                         "to leader: %s",
                                         leaderId, response.getMessage());
                    future.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                future.run(status);
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
        LOG.debug("The node '{}' forward request to leader '{}'",
                  this.nodeId, leaderId);

        RaftClosure<T> future = new RaftClosure<>();
        RpcResponseClosure<T> responseDone = new RpcResponseClosure<T>() {
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
                    BackendException e = new BackendException(
                                         "Current node isn't leader, leader " +
                                         "is [%s], failed to forward request " +
                                         "to leader: %s",
                                         leaderId, commonResponse.getMessage());
                    future.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                future.run(status);
            }
        };
        this.waitRpc(leaderId.getEndpoint(), request, responseDone);
        return future;
    }

    public  <T extends Message> RaftClosure<T>  forwardToQuery(PeerId peerId, Query query,
                                Short shardId, RaftRequests.StoreType storeType) {
        E.checkNotNull(peerId, "peer id");
        LOG.debug("The node {} forward request to leader {}",
                  this.nodeId, peerId);

        StoreCommandRequest.Builder builder = StoreCommandRequest.newBuilder();
        byte[] bytes;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream sOut = new ObjectOutputStream(out);
            sOut.writeObject(query);
            sOut.flush();
            bytes = out.toByteArray();
        } catch (IOException e) {
            throw new BackendException("Failed to get byte array", e);
        }

        builder.setData(ZeroByteStringHelper.wrap(bytes));
        builder.setShardId(shardId);
        builder.setType(storeType);
        builder.setAction(RaftRequests.StoreAction.QUERY);
        StoreCommandRequest request = builder.build();

        RaftClosure<T> future = new RaftClosure<>();
        RpcResponseClosure<T> responseClosure;
        responseClosure = new RpcResponseClosure<T>() {
            @Override
            public void setResponse(T response) {
                StoreCommandResponse resp = (StoreCommandResponse) response;
                if (resp.getStatus()) {
                    future.complete(Status.OK(), () -> response);
                } else {
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e = new BackendException(
                                         "Current node isn't leader, leader " +
                                         "is [%s], failed to forward request " +
                                         "to leader: %s",
                                         peerId, resp.getMessage());
                    future.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                future.run(status);
            }
        };
        this.waitRpc(peerId.getEndpoint(), request, responseClosure);
        return future;
    }

    private <T extends Message> void waitRpc(Endpoint endpoint, Message request,
                                             RpcResponseClosure<T> done) {
        E.checkNotNull(endpoint, "leader endpoint");
        try {
            this.rpcClient.invokeWithDone(endpoint, request, done,
                                          RaftContext.WAIT_RPC_TIMEOUT)
                          .get();
        } catch (InterruptedException e) {
            throw new BackendException("Invoke rpc request was interrupted, " +
                                       "please try again later", e);
        } catch (ExecutionException e) {
            throw new BackendException("Failed to invoke rpc request", e);
        }
    }
}
