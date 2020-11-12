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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.core.Replicator.ReplicatorStateListener;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.util.LZ4Util;
import com.baidu.hugegraph.util.Log;

public class RaftNode {

    private static final Logger LOG = Log.logger(RaftNode.class);

    private final RaftSharedContext context;
    private final Node node;
    private final StoreStateMachine stateMachine;
    private final AtomicReference<LeaderInfo> leaderInfo;
    private final AtomicBoolean started;
    private final AtomicInteger busyCounter;

    public RaftNode(RaftSharedContext context) {
        this.context = context;
        this.stateMachine = new StoreStateMachine(context);
        try {
            this.node = this.initRaftNode();
        } catch (IOException e) {
            throw new BackendException("Failed to init raft node", e);
        }
        this.node.addReplicatorStateListener(new RaftNodeStateListener());
        this.leaderInfo = new AtomicReference<>(LeaderInfo.NO_LEADER);
        this.started = new AtomicBoolean(false);
        this.busyCounter = new AtomicInteger();
    }

    public RaftSharedContext context() {
        return this.context;
    }

    public Node node() {
        return this.node;
    }

    public PeerId nodeId() {
        return this.node.getNodeId().getPeerId();
    }

    public PeerId leaderId() {
        return this.leaderInfo.get().leaderId;
    }

    public boolean selfIsLeader() {
        return this.leaderInfo.get().selfIsLeader;
    }

    public void onLeaderInfoChange(PeerId leaderId, boolean selfIsLeader) {
        leaderId = leaderId != null ? leaderId.copy() : null;
        this.leaderInfo.set(new LeaderInfo(leaderId, selfIsLeader));
    }

    public void shutdown() {
        this.node.shutdown();
    }

    private Node initRaftNode() throws IOException {
        NodeOptions nodeOptions = this.context.nodeOptions();
        nodeOptions.setFsm(this.stateMachine);
        // TODO: When support sharding, groupId needs to be bound to shard Id
        String groupId = this.context.group();
        PeerId endpoint = this.context.endpoint();
        RpcServer rpcServer = this.context.rpcServer();
        RaftGroupService raftGroupService;
        // Shared rpc server
        raftGroupService = new RaftGroupService(groupId, endpoint, nodeOptions,
                                                rpcServer, true);
        // Start node
        return raftGroupService.start(false);
    }

    private void submitCommand(StoreCommand command, StoreClosure closure) {
        // Wait leader elected
        LeaderInfo leaderInfo = this.waitLeaderElected(
                                RaftSharedContext.NO_TIMEOUT);
        if (!leaderInfo.selfIsLeader) {
            this.context.rpcForwarder().forwardToLeader(leaderInfo.leaderId,
                                                        command, closure);
            return;
        }
        // Sleep a while when raft node is busy
        this.waitIfBusy();

        Task task = new Task();
        task.setDone(closure);
        // compress return BytesBuffer
        ByteBuffer buffer = LZ4Util.compress(command.data(),
                                             RaftSharedContext.BLOCK_SIZE)
                                   .forReadWritten()
                                   .asByteBuffer();
        LOG.debug("The bytes size of command(compressed) {} is {}",
                  command.action(), buffer.limit());
        task.setData(buffer);
        LOG.debug("submit to raft node {}", this.node);
        this.node.apply(task);
    }

    public Object submitAndWait(StoreCommand command, StoreClosure future) {
        this.submitCommand(command, future);
        try {
            /*
             * Here will wait future complete, actually the follower has waited
             * in forwardToLeader, written like this to simplify the code
             */
            return future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to wait store command %s",
                                       e, command);
        }
    }

    protected LeaderInfo waitLeaderElected(int timeout) {
        String group = this.context.group();
        LeaderInfo leaderInfo = this.leaderInfo.get();
        if (leaderInfo.leaderId != null) {
            return leaderInfo;
        }
        long beginTime = System.currentTimeMillis();
        while (leaderInfo.leaderId == null) {
            try {
                Thread.sleep(RaftSharedContext.POLL_INTERVAL);
            } catch (InterruptedException e) {
                throw new BackendException(
                          "Waiting for raft group '%s' election is interrupted",
                          e, group);
            }
            long consumedTime = System.currentTimeMillis() - beginTime;
            if (timeout > 0 && consumedTime >= timeout) {
                throw new BackendException(
                          "Waiting for raft group '{}' election timeout({}ms)",
                          group, consumedTime);
            }
            LOG.warn("Waiting for raft group '{}' election cost {}s",
                     group, consumedTime / 1000.0);
            leaderInfo = this.leaderInfo.get();
            assert leaderInfo != null;
        }
        return leaderInfo;
    }

    protected void waitStarted(int timeout) {
        String group = this.context.group();
        ReadIndexClosure readIndexClosure = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                RaftNode.this.started.set(status.isOk());
            }
        };
        long beginTime = System.currentTimeMillis();
        while (true) {
            this.node.readIndex(BytesUtil.EMPTY_BYTES, readIndexClosure);
            if (this.started.get()) {
                break;
            }
            try {
                Thread.sleep(RaftSharedContext.POLL_INTERVAL);
            } catch (InterruptedException e) {
                throw new BackendException("Try to sleep a while for waiting " +
                                           "heartbeat is interrupted", e);
            }
            long consumedTime = System.currentTimeMillis() - beginTime;
            if (timeout > 0 && consumedTime >= timeout) {
                throw new BackendException(
                          "Waiting for raft group '{}' heartbeat timeout({}ms)",
                          group, consumedTime);
            }
            LOG.warn("Waiting for raft group '{}' heartbeat cost {}s",
                     group, consumedTime / 1000.0);
        }
    }

    private void waitIfBusy() {
        int counter = this.busyCounter.get();
        if (counter <= 0) {
            return;
        }
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
                        counter = this.busyCounter.decrementAndGet();
                        LOG.info("Decrease busy counter: [{}]", counter);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return String.format("[%s-%s]", this.context.group(), this.nodeId());
    }

    private class RaftNodeStateListener implements ReplicatorStateListener {

        // unit is ms
        private static final long ERROR_PRINT_INTERVAL = 60 * 1000;
        private volatile long lastPrintTime;

        public RaftNodeStateListener() {
            this.lastPrintTime = 0L;
        }

        @Override
        public void onCreated(PeerId peer) {
            LOG.info("The node {} replicator has created", peer);
        }

        @Override
        public void onError(PeerId peer, Status status) {
            long now = System.currentTimeMillis();
            if (now - this.lastPrintTime >= ERROR_PRINT_INTERVAL) {
                LOG.warn("Replicator meet error: {}", status);
                this.lastPrintTime = now;
            }
            if (this.isWriteBufferOverflow(status)) {
                // increment busy counter
                int count = RaftNode.this.busyCounter.incrementAndGet();
                LOG.info("Increase busy counter: [{}]", count);
            }
        }

        private boolean isWriteBufferOverflow(Status status) {
            String expectMsg = "maybe write overflow";
            return RaftError.EINTERNAL == status.getRaftError() &&
                   status.getErrorMsg() != null &&
                   status.getErrorMsg().contains(expectMsg);
        }

        /**
         * Maybe useful in the future
         */
        private boolean isRpcTimeout(Status status) {
            String expectMsg = "Invoke timeout";
            return RaftError.EINTERNAL == status.getRaftError() &&
                   status.getErrorMsg() != null &&
                   status.getErrorMsg().contains(expectMsg);
        }

        @Override
        public void onDestroyed(PeerId peer) {
            LOG.warn("Replicator {} prepare to offline", peer);
        }
    }

    /**
     * Jraft Node.getLeaderId() and Node.isLeader() is not always consistent,
     * We define this class to manage leader info by ourselves
     */
    private static class LeaderInfo {

        private static final LeaderInfo NO_LEADER = new LeaderInfo(null, false);

        private final PeerId leaderId;
        private final boolean selfIsLeader;

        public LeaderInfo(PeerId leaderId, boolean selfIsLeader) {
            this.leaderId = leaderId;
            this.selfIsLeader = selfIsLeader;
        }
    }
}
