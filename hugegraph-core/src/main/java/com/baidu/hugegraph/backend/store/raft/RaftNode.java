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
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class RaftNode {

    private static final Logger LOG = Log.logger(RaftNode.class);

    private final StateMachine fsm;
    private final Node node;

    public RaftNode(BackendStore store, RaftSharedComponent component) {
        this.fsm = new BackendStoreStateMachine(store);
        try {
            this.node = this.initRaftNode(store, component);
        } catch (IOException e) {
            throw new BackendException("Failed to init raft node", e);
        }
    }

    public Node node() {
        return this.node;
    }

    public Node initRaftNode(BackendStore store, RaftSharedComponent component)
                             throws IOException {
        final HugeConfig config = component.config();
        String storePath = store.database() + "-" + store.store();
        // TODO: When sharding is supported, groupId needs to be bound to
        //  the shard Id
        final String groupId = storePath;
        final PeerId serverId = new PeerId();
        serverId.parse(config.get(CoreOptions.RAFT_PEERID));

        NodeOptions nodeOptions = this.initNodeOptions(config);
        nodeOptions.setFsm(this.fsm);
        String raftLogPath = config.get(CoreOptions.RAFT_LOG_PATH);
        String logUri = Paths.get(raftLogPath, "log", storePath).toString();
        FileUtils.forceMkdir(new File(logUri));
        nodeOptions.setLogUri(logUri);
        String metaUri = Paths.get(raftLogPath, "raft_meta", storePath)
                              .toString();
        FileUtils.forceMkdir(new File(metaUri));
        nodeOptions.setRaftMetaUri(metaUri);
        String snapshotUri = Paths.get(raftLogPath, "snapshot", storePath)
                                  .toString();
        FileUtils.forceMkdir(new File(snapshotUri));
        // TODO：为方便调试，暂时关闭快照功能
        snapshotUri = null;
        nodeOptions.setSnapshotUri(snapshotUri);

        // 这里让 raft RPC 和业务 RPC 使用同一个 RPC server, 通常也可以分开
        RpcServer rpcServer = component.rpcServer();
        // 初始化 raft group 服务框架
        RaftGroupService raftGroupService;
        raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions,
                                                rpcServer, true);
        // 启动
        return raftGroupService.start(false);
    }

    public NodeOptions initNodeOptions(HugeConfig config) {
        final NodeOptions nodeOptions = new NodeOptions();
        // 为了测试,调整 snapshot 间隔等参数
        // 设置选举超时时间为 1 秒
        nodeOptions.setElectionTimeoutMs(10000);
        // 关闭 CLI 服务
        nodeOptions.setDisableCli(false);
        // 每隔60秒做一次 snapshot
        nodeOptions.setSnapshotIntervalSecs(60);
        // 解析参数
        final PeerId serverId = new PeerId();
        String serverIdStr = config.get(CoreOptions.RAFT_PEERID);
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        final Configuration initConf = new Configuration();
        String initConfStr = config.get(CoreOptions.RAFT_GROUP_PEERS);
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // 设置初始集群配置
        nodeOptions.setInitialConf(initConf);
        return nodeOptions;
    }

    public boolean isLeader() {
        return this.node.isLeader();
    }

    public void submitCommand(StoreCommand command, StoreClosure closure) {
        if (!this.isLeader()) {
            PeerId leaderId = this.node.getLeaderId();
            closure.failure(new BackendException("Current node isn't leader, " +
                                                 "leader is %s", leaderId));
            return;
        }

        Task task = new Task();
        task.setData(ByteBuffer.wrap(command.toBytes()));
        task.setDone(closure);
        LOG.debug("submit to raft node {}", this.node);
        this.node.apply(task);
    }
}
