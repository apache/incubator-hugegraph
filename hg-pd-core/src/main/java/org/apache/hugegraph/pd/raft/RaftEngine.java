/*
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

package org.apache.hugegraph.pd.raft;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftEngine {
    private static final RaftEngine instance = new RaftEngine();
    private final String groupId = "pd_raft";
    private final RaftStateMachine stateMachine;
    private PDConfig.Raft config;
    private RaftGroupService raftGroupService;
    private RpcServer rpcServer;
    private Node raftNode;
    private RaftRpcClient raftRpcClient;

    public RaftEngine() {
        this.stateMachine = new RaftStateMachine();
    }

    public static RaftEngine getInstance() {
        return instance;
    }

    public boolean init(PDConfig.Raft config) {
        if (this.raftNode != null) return false;
        this.config = config;

        raftRpcClient = new RaftRpcClient();
        raftRpcClient.init(new RpcOptions());

        String raftPath = config.getDataPath() + "/" + groupId;
        new File(raftPath).mkdirs();

        new File(config.getDataPath()).mkdirs();
        Configuration initConf = new Configuration();
        initConf.parse(config.getPeersList());
        if (config.isEnable() && config.getPeersList().length() < 3) {
            log.error(
                    "The RaftEngine parameter is incorrect." +
                    " When RAFT is enabled, the number of peers " +
                    "cannot be less than 3");
        }
        // 设置Node参数，包括日志存储路径和状态机实例
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setEnableMetrics(true);
        // 日志路径
        nodeOptions.setLogUri(raftPath + "/log");
        // raft元数据路径
        nodeOptions.setRaftMetaUri(raftPath + "/meta");
        // 快照路径
        nodeOptions.setSnapshotUri(raftPath + "/snapshot");
        // 初始集群
        nodeOptions.setInitialConf(initConf);
        // 快照时间间隔
        nodeOptions.setSnapshotIntervalSecs(config.getSnapshotInterval());

        nodeOptions.setRpcConnectTimeoutMs(config.getRpcTimeout());
        nodeOptions.setRpcDefaultTimeout(config.getRpcTimeout());
        nodeOptions.setRpcInstallSnapshotTimeout(config.getRpcTimeout());
        // 设置raft配置
        RaftOptions raftOptions = nodeOptions.getRaftOptions();

        nodeOptions.setEnableMetrics(true);

        final PeerId serverId = JRaftUtils.getPeerId(config.getAddress());

        rpcServer = createRaftRpcServer(config.getAddress());
        // 构建raft组并启动raft
        this.raftGroupService = new RaftGroupService(groupId, serverId,
                                                     nodeOptions, rpcServer, true);
        this.raftNode = raftGroupService.start(false);
        log.info("RaftEngine start successfully: id = {}, peers list = {}", groupId,
                 nodeOptions.getInitialConf().getPeers());
        return this.raftNode != null;
    }

    /**
     * 创建raft rpc server，用于pd之间通讯
     */
    private RpcServer createRaftRpcServer(String raftAddr) {
        Endpoint endpoint = JRaftUtils.getEndPoint(raftAddr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
        RaftRpcProcessor.registerProcessor(rpcServer, this);
        rpcServer.init(null);
        return rpcServer;
    }

    public void shutDown() {
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                this.raftNode = null;
                ThrowUtil.throwException(e);
            }
            this.raftGroupService = null;
        }
        if (this.rpcServer != null) {
            this.rpcServer.shutdown();
            this.rpcServer = null;
        }
        if (this.raftNode != null) {
            this.raftNode.shutdown();
        }
        this.raftNode = null;
    }

    public boolean isLeader() {
        return this.raftNode.isLeader(true);
    }

    /**
     * 添加Raft任务，grpc通过该接口给raft发送数据
     */
    public void addTask(Task task) {
        if (!isLeader()) {
            KVStoreClosure closure = (KVStoreClosure) task.getDone();
            closure.setError(Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.NOT_LEADER).build());
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        this.raftNode.apply(task);
    }

    public void addStateListener(RaftStateListener listener) {
        this.stateMachine.addStateListener(listener);
    }

    public void addTaskHandler(RaftTaskHandler handler) {
        this.stateMachine.addTaskHandler(handler);
    }

    public PDConfig.Raft getConfig() {
        return this.config;
    }

    public PeerId getLeader() {
        return raftNode.getLeaderId();
    }

    /**
     * 向leader发消息，获取grpc地址；
     */
    public String getLeaderGrpcAddress() throws ExecutionException, InterruptedException {
        if (isLeader()) return config.getGrpcAddress();
        return raftRpcClient.getGrpcAddress(
                                    raftNode.getLeaderId().getEndpoint().toString())
                            .get().getGrpcAddress();
    }

    public Metapb.Member getLocalMember() {
        Metapb.Member.Builder builder = Metapb.Member.newBuilder();
        builder.setClusterId(config.getClusterId());
        builder.setRaftUrl(config.getAddress());
        builder.setDataPath(config.getDataPath());
        builder.setGrpcUrl(config.getGrpcAddress());
        builder.setState(Metapb.StoreState.Up);
        return builder.build();
    }

    public List<Metapb.Member> getMembers() throws ExecutionException, InterruptedException {
        List<Metapb.Member> members = new ArrayList<>();

        List<PeerId> peers = raftNode.listPeers();
        for (PeerId peerId : peers) {
            Metapb.Member.Builder builder = Metapb.Member.newBuilder();
            builder.setClusterId(config.getClusterId());
            CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                    raftRpcClient.getGrpcAddress(peerId.getEndpoint().toString());
            try {
                if (future.isCompletedExceptionally()) {
                    log.error("failed to getGrpcAddress of {}",
                              peerId.getEndpoint().toString());
                    builder.setState(Metapb.StoreState.Offline);
                    builder.setRaftUrl(peerId.getEndpoint().toString());
                    members.add(builder.build());
                } else {
                    RaftRpcProcessor.GetMemberResponse response = future.get();
                    builder.setState(Metapb.StoreState.Up);
                    builder.setRaftUrl(response.getRaftAddress());
                    builder.setDataPath(response.getDatePath());
                    builder.setGrpcUrl(response.getGrpcAddress());
                    builder.setRestUrl(response.getRestAddress());
                    members.add(builder.build());
                }
            } catch (Exception e) {
                log.error("failed to getGrpcAddress of {}. {}",
                          peerId.getEndpoint().toString(), e);
                builder.setState(Metapb.StoreState.Offline);
                builder.setRaftUrl(peerId.getEndpoint().toString());
                members.add(builder.build());
            }

        }
        return members;
    }

    public Status changePeerList(String peerList) {
        AtomicReference<Status> result = new AtomicReference<>();
        try {
            String[] peers = peerList.split(",", -1);
            if ((peers.length & 1) != 1) {
                throw new PDException(-1, "the number of peer list must be odd.");
            }
            Configuration newPeers = new Configuration();
            newPeers.parse(peerList);
            CountDownLatch latch = new CountDownLatch(1);
            this.raftNode.changePeers(newPeers, status -> {
                result.set(status);
                latch.countDown();
            });
            latch.await();
        } catch (Exception e) {
            log.error("failed to changePeerList to {},{}", peerList, e);
            result.set(new Status(-1, e.getMessage()));
        }
        return result.get();
    }

    public PeerId waitingForLeader(long timeOut) {
        PeerId leader = getLeader();
        if (leader != null) {
            return leader;
        }

        synchronized (this) {
            leader = getLeader();
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < timeOut) && (leader == null)) {
                try {
                    this.wait(1000);
                } catch (InterruptedException e) {
                    log.error("Raft wait for leader exception", e);
                }
                leader = getLeader();
            }
            return leader;
        }

    }
}
