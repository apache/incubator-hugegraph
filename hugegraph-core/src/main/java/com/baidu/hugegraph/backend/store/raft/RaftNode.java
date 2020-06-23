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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.CodeUtil;
import com.baidu.hugegraph.util.Log;

public class RaftNode {

    private static final Logger LOG = Log.logger(RaftNode.class);

    private final StoreStateMachine stateMachine;
    private final Node node;

    public RaftNode(BackendStore store, RaftSharedContext context) {
        this.stateMachine = new StoreStateMachine(store, context);
        try {
            this.node = this.initRaftNode(store, context);
        } catch (IOException e) {
            throw new BackendException("Failed to init raft node", e);
        }
        this.stateMachine.nodeId(this.node.getNodeId());
    }

    public Node node() {
        return this.node;
    }

    private Node initRaftNode(BackendStore store, RaftSharedContext context)
                              throws IOException {
        HugeConfig config = context.config();
        String storePath = store.database() + "-" + store.store();
        // TODO: When support sharding, groupId needs to be bound to shard Id
        String groupId = storePath;
        PeerId serverId = new PeerId();
        serverId.parse(config.get(CoreOptions.RAFT_PEERID));

        NodeOptions nodeOptions = this.initNodeOptions(config);
        nodeOptions.setFsm(this.stateMachine);

        String raftPath = config.get(CoreOptions.RAFT_PATH);
        String logUri = Paths.get(raftPath, "log", storePath).toString();
        FileUtils.forceMkdir(new File(logUri));
        nodeOptions.setLogUri(logUri);

        String metaUri = Paths.get(raftPath, "meta", storePath).toString();
        FileUtils.forceMkdir(new File(metaUri));
        nodeOptions.setRaftMetaUri(metaUri);

        if (config.get(CoreOptions.RAFT_USE_SNAPSHOT)) {
            String snapshotUri = Paths.get(raftPath, "snapshot", storePath)
                                      .toString();
            FileUtils.forceMkdir(new File(snapshotUri));
            nodeOptions.setSnapshotUri(snapshotUri);
        }

        RaftOptions raftOptions = nodeOptions.getRaftOptions();
        raftOptions.setDisruptorBufferSize(32768);

        nodeOptions.setRpcProcessorThreadPoolSize(48);
        nodeOptions.setEnableMetrics(false);

        RaftGroupService raftGroupService;
        // Shared rpc server
        raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions,
                                                context.rpcServer(), true);
        // Start node
        return raftGroupService.start(false);
    }

    private NodeOptions initNodeOptions(HugeConfig config) {
        final NodeOptions nodeOptions = new NodeOptions();
        int electionTimeout = config.get(CoreOptions.RAFT_ELECTION_TIMEOUT_MS);
        nodeOptions.setElectionTimeoutMs(electionTimeout);
        nodeOptions.setDisableCli(false);

        int snapshotInterval = config.get(CoreOptions.RAFT_SNAPSHOT_INTERVAL_SEC);
        nodeOptions.setSnapshotIntervalSecs(snapshotInterval);

        PeerId serverId = new PeerId();
        String serverIdStr = config.get(CoreOptions.RAFT_PEERID);
        if (!serverId.parse(serverIdStr)) {
            throw new HugeException("Failed to parse serverId %s", serverIdStr);
        }

        Configuration initConf = new Configuration();
        String initConfStr = config.get(CoreOptions.RAFT_GROUP_PEERS);
        if (!initConf.parse(initConfStr)) {
            throw new HugeException("Failed to parse initConf %s", initConfStr);
        }
        nodeOptions.setInitialConf(initConf);
        return nodeOptions;
    }

    public void submitCommand(StoreCommand command, StoreClosure closure) {
        if (!this.node.isLeader()) {
            PeerId leaderId = this.node.getLeaderId();
            closure.failure(new BackendException("Current node isn't leader, " +
                                                 "leader is [%s]", leaderId));
            return;
        }

        Task task = new Task();
        task.setDone(closure);
        // compress return BytesBuffer
        ByteBuffer buffer = CodeUtil.compress(command.toBytes()).asByteBuffer();
        LOG.debug("HG ==> The bytes size of command {} is {}",
                  command.action(), buffer.limit());
        task.setData(buffer);
        LOG.debug("submit to raft node {}", this.node);
        this.node.apply(task);
    }
}
