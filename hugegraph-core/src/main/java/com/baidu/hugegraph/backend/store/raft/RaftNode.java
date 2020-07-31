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

import static com.baidu.hugegraph.backend.store.raft.RaftSharedContext.BUSY_SLEEP_FACTOR;
import static com.baidu.hugegraph.backend.store.raft.RaftSharedContext.WAIT_RPC_TIMEOUT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.Replicator.ReplicatorStateListener;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.ClientService;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.StoreAction;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.StoreCommandRequest;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.StoreCommandResponse;
import com.baidu.hugegraph.util.CodeUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.protobuf.ZeroByteStringHelper;

public class RaftNode {

    private static final Logger LOG = Log.logger(RaftNode.class);

    private final String group;
    private final StoreStateMachine stateMachine;
    private final Node node;

    private final Object electedLock;
    private AtomicBoolean elected;
    private AtomicBoolean started;
    private final AtomicInteger busyCounter;

    public RaftNode(String group, BackendStore store,
                    RaftSharedContext context) {
        this.group = group;
        this.stateMachine = new StoreStateMachine(store, this, context);
        try {
            this.node = this.initRaftNode(store, context);
        } catch (IOException e) {
            throw new BackendException("Failed to init raft node", e);
        }
        this.node.addReplicatorStateListener(new RaftNodeStateListener());
        this.electedLock = new Object();
        this.elected = new AtomicBoolean(false);
        this.started = new AtomicBoolean(false);
        this.busyCounter = new AtomicInteger();
    }

    public String group() {
        return this.group;
    }

    public Node node() {
        return this.node;
    }

    public NodeId nodeId() {
        return this.node.getNodeId();
    }

    public void shutdown() {
        this.node.shutdown();
    }

    private Node initRaftNode(BackendStore store, RaftSharedContext context)
                              throws IOException {
        String storePath = store.database() + "-" + store.store();
        NodeOptions nodeOptions = context.nodeOptions(storePath);
        nodeOptions.setFsm(this.stateMachine);

        // TODO: When support sharding, groupId needs to be bound to shard Id
        String groupId = storePath;
        PeerId endpoint = context.endpoint();

        RaftGroupService raftGroupService;
        // Shared rpc server
        raftGroupService = new RaftGroupService(groupId, endpoint, nodeOptions,
                                                context.rpcServer(), true);
        // Start node
        return raftGroupService.start(false);
    }

    private void submitCommand(StoreCommand command, StoreClosure closure) {
        // Wait leader elected
        this.waitLeaderElected(-1);

        if (!this.node.isLeader()) {
            this.forwardToLeader(command, closure);
            return;
        }
        // Sleep a while when raft node is busy
        this.waitIfBusy();

        Task task = new Task();
        task.setDone(closure);
        // compress return BytesBuffer
        ByteBuffer buffer = CodeUtil.compress(command.toBytes()).asByteBuffer();
        LOG.debug("The bytes size of command(compressed) {} is {}",
                  command.action(), buffer.limit());
        task.setData(buffer);
        LOG.debug("submit to raft node {}", this.node);
        this.node.apply(task);
    }

    public Object submitAndWait(StoreCommand command, StoreClosure closure) {
        this.submitCommand(command, closure);
        if (!this.node.isLeader()) {
            return null;
        }
        try {
            // Here will wait future complete
            return closure.waitFinished();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new BackendException(t);
        }
    }

    public void onElected(boolean value) {
        synchronized(this.electedLock) {
            this.elected.set(value);
            this.electedLock.notify();
        }
    }

    protected void waitLeaderElected(int timeout) {
        if (this.node.getLeaderId() != null) {
            return;
        }
        long beginTime = System.currentTimeMillis();
        int internalTimeout = 3000;
        synchronized(this.electedLock) {
            while (!this.elected.get()) {
                try {
                    this.electedLock.wait(internalTimeout);
                } catch (InterruptedException e) {
                    throw new BackendException(
                              "Wait raft group '%s' election error",
                              e, this.group(), "election");
                }
                long consumedTime = System.currentTimeMillis() - beginTime;
                if (timeout != -1 && consumedTime >= timeout) {
                    throw new BackendException(
                              "Wait raft group '{}' election timeout({}ms)",
                              this.group(), "", consumedTime);
                }
                LOG.warn("Waiting raft group '{}' election cost {}s",
                         this.group(), consumedTime / 1000.0);
            }
        }
    }

    protected void waitStarted(int timeout) {
        ReadIndexClosure readIndexClosure = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    RaftNode.this.started.set(true);
                } else {
                    RaftNode.this.started.set(false);
                }
            }
        };
        long beginTime = System.currentTimeMillis();
        int internalTimeout = 3000;
        while (true) {
            this.node.readIndex(BytesUtil.EMPTY_BYTES, readIndexClosure);
            if (this.started.get()) {
                break;
            }
            try {
                Thread.sleep(internalTimeout);
            } catch (InterruptedException ex) {
                throw new BackendException("Try to sleep a while for waiting " +
                                           "heartbeat is interrupted");
            }
            long consumedTime = System.currentTimeMillis() - beginTime;
            if (timeout != -1 && consumedTime >= timeout) {
                throw new BackendException(
                          "Wait raft group '{}' heartbeat timeout({}ms)",
                          this.group(), consumedTime);
            }
            LOG.warn("Waiting raft group '{}' heartbeat cost {}s",
                     this.group(), consumedTime / 1000.0);
        }
    }

    private void waitIfBusy() {
        int counter = this.busyCounter.get();
        if (counter <= 0) {
            return;
        }
        // TODO：should sleep or throw exception directly?
        // It may lead many thread sleep, but this is exactly what I want
        long time = counter * BUSY_SLEEP_FACTOR;
        LOG.info("The node {} will sleep {} ms", this.node, time);
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // throw busy exception
            throw new BackendException("The raft backend store is busy");
        } finally {
            if (this.busyCounter.get() > 0) {
                synchronized (this) {
                    if (this.busyCounter.get() > 0) {
                        this.busyCounter.decrementAndGet();
                    }
                }
            }
        }
    }

    private void forwardToLeader(StoreCommand command, StoreClosure closure) {
        assert !this.node.isLeader();
        PeerId leaderId = this.node.getLeaderId();
        E.checkNotNull(leaderId, "leader id");
        LOG.debug("The node {} forward request to leader {}",
                  this.node.getNodeId(), leaderId);

        StoreCommandRequest.Builder builder = StoreCommandRequest.newBuilder();
        builder.setGroupId(this.group);
        builder.setAction(StoreAction.valueOf(command.action().code()));
        builder.setData(ZeroByteStringHelper.wrap(command.data()));
        StoreCommandRequest request = builder.build();

        RpcResponseClosure<StoreCommandResponse> responseClosure;
        responseClosure = new RpcResponseClosure<StoreCommandResponse>() {
            @Override
            public void setResponse(StoreCommandResponse resp) {
                if (resp.getStatus()) {
                    LOG.debug("StoreCommandResponse status ok");
                    closure.complete(Status.OK(), null);
                } else {
                    LOG.debug("StoreCommandResponse status error");
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    closure.failure(status, new BackendException(
                                    "Current node isn't leader, leader is " +
                                    "[%s], failed to forward request to " +
                                    "leader: %s", leaderId, resp.getMessage()));
                }
            }

            @Override
            public void run(Status status) {
                closure.run(status);
            }
        };

        ClientService rpcClient = ((NodeImpl) this.node).getRpcService();
        E.checkNotNull(rpcClient, "rpc client");
        E.checkNotNull(leaderId.getEndpoint(), "leader endpoint");
        try {
            rpcClient.invokeWithDone(leaderId.getEndpoint(), request,
                                     responseClosure, WAIT_RPC_TIMEOUT)
                     .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new BackendException("Failed to invoke rpc request", e);
        }
    }

    private class RaftNodeStateListener implements ReplicatorStateListener {

        @Override
        public void onCreated(PeerId peer) {
            LOG.info("The node {} replicator has created", peer);
        }

        @Override
        public void onError(PeerId peer, Status status) {
            LOG.warn("Replicator meet error: {}", status);
            if (this.isWriteBufferOverflow(status)) {
                // increment busy counter
                int count = RaftNode.this.busyCounter.incrementAndGet();
                LOG.info("Busy counter: [{}]", count);
            }
        }

        private boolean isWriteBufferOverflow(Status status) {
            String expectMsg = "maybe write overflow";
            return RaftError.EINTERNAL == status.getRaftError() &&
                   status.getErrorMsg() != null &&
                   status.getErrorMsg().contains(expectMsg);
        }

        @Override
        public void onDestroyed(PeerId peer) {
            LOG.warn("The node {} prepare to offline", peer);
        }
    }
}
