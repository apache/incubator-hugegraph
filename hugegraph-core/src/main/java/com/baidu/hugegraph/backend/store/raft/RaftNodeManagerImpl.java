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

import java.util.Map;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.SetLeaderRequest;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class RaftNodeManagerImpl implements RaftNodeManager {

    private final String group;
    private final RaftNode raftNode;

    public RaftNodeManagerImpl(String group, RaftNode raftNode) {
        this.group = group;
        this.raftNode = raftNode;
    }

    @Override
    public Map<String, String> getLeader() {
        PeerId leaderId = this.raftNode.leaderId();
        E.checkState(leaderId != null,
                     "There is no leader for raft group %s", this.group);
        return ImmutableMap.of(this.group, leaderId.toString());
    }

    @Override
    public void transferLeaderTo(String endpoint) {
        PeerId peerId = PeerId.parsePeer(endpoint);
        Status status = this.raftNode.node().transferLeadershipTo(peerId);
        if (!status.isOk()) {
            throw new BackendException(
                      "Failed to transafer leader to '%s', raft error: %s",
                      peerId, status.getErrorMsg());
        }
    }

    @Override
    public void setLeader(String endpoint) {
        PeerId newLeaderId = PeerId.parsePeer(endpoint);
        Node node = this.raftNode.node();
        // No need to re-elect if already is new leader
        if (node.getLeaderId().equals(newLeaderId)) {
            return;
        }
        if (this.raftNode.selfIsLeader()) {
            // If current node is the leader, transfer directly
            this.transferLeaderTo(endpoint);
        } else {
            // If current node is not leader, forward request to leader
            SetLeaderRequest request = SetLeaderRequest.newBuilder()
                                                       .setEndpoint(endpoint)
                                                       .build();
            RaftClosure future = new RaftClosure();
            this.raftNode.forwardToLeader(this.raftNode.leaderId(),
                                          request, future);
            try {
                future.waitFinished();
            } catch (Throwable e) {
                throw new BackendException("Failed to set leader to '%s'",
                                           e, endpoint);
            }
        }
    }

    @Override
    public void addPeer(String endpoint) {
        PeerId peerId = PeerId.parsePeer(endpoint);
        Node node = this.raftNode.node();
        RaftClosure future = new RaftClosure();
        node.addPeer(peerId, future);
        try {
            future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to add peer '%s'", e, endpoint);
        }
    }

    @Override
    public void removePeer(String endpoint) {
        PeerId peerId = PeerId.parsePeer(endpoint);
        Node node = this.raftNode.node();
        RaftClosure future = new RaftClosure();
        node.removePeer(peerId, future);
        try {
            future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to remove peer '%s'",
                                       e, endpoint);
        }
    }
}
