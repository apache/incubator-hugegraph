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
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.BoltRaftRpcFactory;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.raft.rpc.ListPeersProcessor;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import com.baidu.hugegraph.backend.store.raft.rpc.RpcForwarder;
import com.baidu.hugegraph.backend.store.raft.rpc.SetLeaderProcessor;
import com.baidu.hugegraph.backend.store.raft.rpc.StoreCommandProcessor;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;

public final class RaftSharedContext {

    private static final Logger LOG = Log.logger(RaftSharedContext.class);

    // unit is ms
    public static final int NO_TIMEOUT = -1;
    public static final int POLL_INTERVAL = 3000;
    public static final int WAIT_RAFT_LOG_TIMEOUT = 30 * 60 * 1000;
    public static final int WAIT_LEADER_TIMEOUT = 5 * 60 * 1000;
    public static final int BUSY_SLEEP_FACTOR = 3 * 1000;
    public static final int WAIT_RPC_TIMEOUT = 30 * 60 * 1000;
    // compress block size
    public static final int BLOCK_SIZE = 4096;

    public static final String DEFAULT_GROUP = "default";

    private final HugeGraphParams params;
    private final String schemaStoreName;
    private final String graphStoreName;
    private final String systemStoreName;
    private final RaftBackendStore[] stores;
    private final RpcServer rpcServer;
    @SuppressWarnings("unused")
    private final ExecutorService readIndexExecutor;
    private final ExecutorService snapshotExecutor;
    private final ExecutorService backendExecutor;

    private RaftNode raftNode;
    private RaftGroupManager raftGroupManager;
    private RpcForwarder rpcForwarder;

    public RaftSharedContext(HugeGraphParams params) {
        this.params = params;
        HugeConfig config = params.configuration();

        this.schemaStoreName = config.get(CoreOptions.STORE_SCHEMA);
        this.graphStoreName = config.get(CoreOptions.STORE_GRAPH);
        this.systemStoreName = config.get(CoreOptions.STORE_SYSTEM);
        this.stores = new RaftBackendStore[StoreType.SIZE.getNumber()];
        this.rpcServer = this.initAndStartRpcServer();
        if (config.get(CoreOptions.RAFT_SAFE_READ)) {
            int readIndexThreads = config.get(CoreOptions.RAFT_READ_INDEX_THREADS);
            this.readIndexExecutor = this.createReadIndexExecutor(readIndexThreads);
        } else {
            this.readIndexExecutor = null;
        }
        if (config.get(CoreOptions.RAFT_USE_SNAPSHOT)) {
            this.snapshotExecutor = this.createSnapshotExecutor(4);
        } else {
            this.snapshotExecutor = null;
        }
        int backendThreads = config.get(CoreOptions.RAFT_BACKEND_THREADS);
        this.backendExecutor = this.createBackendExecutor(backendThreads);

        this.raftNode = null;
        this.raftGroupManager = null;
        this.rpcForwarder = null;
        this.registerRpcRequestProcessors();
    }

    private void registerRpcRequestProcessors() {
        this.rpcServer.registerProcessor(new StoreCommandProcessor(this));
        this.rpcServer.registerProcessor(new SetLeaderProcessor(this));
        this.rpcServer.registerProcessor(new ListPeersProcessor(this));
    }

    public void initRaftNode() {
        this.raftNode = new RaftNode(this);
        this.rpcForwarder = new RpcForwarder(this.raftNode);
        this.raftGroupManager = new RaftGroupManagerImpl(this);
    }

    public void waitRaftNodeStarted() {
        RaftNode node = this.node();
        node.waitLeaderElected(RaftSharedContext.WAIT_LEADER_TIMEOUT);
        if (node.selfIsLeader()) {
            node.waitStarted(RaftSharedContext.NO_TIMEOUT);
        }
    }

    public void close() {
        LOG.info("Stopping raft nodes");
        this.rpcServer.shutdown();
    }

    public RaftNode node() {
        return this.raftNode;
    }

    public RpcForwarder rpcForwarder() {
        return this.rpcForwarder;
    }

    public RaftGroupManager raftNodeManager(String group) {
        E.checkArgument(DEFAULT_GROUP.equals(group),
                        "The group must be '%s' now, actual is '%s'",
                        DEFAULT_GROUP, group);
        return this.raftGroupManager;
    }

    public RpcServer rpcServer() {
        return this.rpcServer;
    }

    public String group() {
        return DEFAULT_GROUP;
    }

    public void addStore(StoreType type, RaftBackendStore store) {
        this.stores[type.getNumber()] = store;
    }

    public StoreType storeType(String store) {
        if (this.schemaStoreName.equals(store)) {
            return StoreType.SCHEMA;
        } else if (this.graphStoreName.equals(store)) {
            return StoreType.GRAPH;
        } else {
            assert this.systemStoreName.equals(store);
            return StoreType.SYSTEM;
        }
    }

    protected RaftBackendStore[] stores() {
        return this.stores;
    }

    public BackendStore originStore(StoreType storeType) {
        RaftBackendStore raftStore = this.stores[storeType.getNumber()];
        E.checkState(raftStore != null,
                     "The raft store of type %s shouldn't be null", storeType);
        return raftStore.originStore();
    }

    public NodeOptions nodeOptions() throws IOException {
        HugeConfig config = this.config();
        PeerId selfId = new PeerId();
        selfId.parse(config.get(CoreOptions.RAFT_ENDPOINT));

        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setEnableMetrics(false);
        nodeOptions.setRpcProcessorThreadPoolSize(
                    config.get(CoreOptions.RAFT_RPC_THREADS));
        nodeOptions.setRpcConnectTimeoutMs(
                    config.get(CoreOptions.RAFT_RPC_CONNECT_TIMEOUT));
        nodeOptions.setRpcDefaultTimeout(
                    config.get(CoreOptions.RAFT_RPC_TIMEOUT));

        int electionTimeout = config.get(CoreOptions.RAFT_ELECTION_TIMEOUT);
        nodeOptions.setElectionTimeoutMs(electionTimeout);
        nodeOptions.setDisableCli(false);

        int snapshotInterval = config.get(CoreOptions.RAFT_SNAPSHOT_INTERVAL);
        nodeOptions.setSnapshotIntervalSecs(snapshotInterval);

        Configuration groupPeers = new Configuration();
        String groupPeersStr = config.get(CoreOptions.RAFT_GROUP_PEERS);
        if (!groupPeers.parse(groupPeersStr)) {
            throw new HugeException("Failed to parse group peers %s",
                                    groupPeersStr);
        }
        nodeOptions.setInitialConf(groupPeers);

        String raftPath = config.get(CoreOptions.RAFT_PATH);
        String logUri = Paths.get(raftPath, "log").toString();
        FileUtils.forceMkdir(new File(logUri));
        nodeOptions.setLogUri(logUri);

        String metaUri = Paths.get(raftPath, "meta").toString();
        FileUtils.forceMkdir(new File(metaUri));
        nodeOptions.setRaftMetaUri(metaUri);

        if (config.get(CoreOptions.RAFT_USE_SNAPSHOT)) {
            String snapshotUri = Paths.get(raftPath, "snapshot").toString();
            FileUtils.forceMkdir(new File(snapshotUri));
            nodeOptions.setSnapshotUri(snapshotUri);
        }

        RaftOptions raftOptions = nodeOptions.getRaftOptions();
        /*
         * NOTE: if buffer size is too small(<=1024), will throw exception
         * "LogManager is busy, disk queue overload"
         */
        raftOptions.setApplyBatch(config.get(CoreOptions.RAFT_APPLY_BATCH));
        raftOptions.setDisruptorBufferSize(
                    config.get(CoreOptions.RAFT_QUEUE_SIZE));
        raftOptions.setDisruptorPublishEventWaitTimeoutSecs(
                    config.get(CoreOptions.RAFT_QUEUE_PUBLISH_TIMEOUT));
        raftOptions.setReplicatorPipeline(
                    config.get(CoreOptions.RAFT_REPLICATOR_PIPELINE));
        raftOptions.setOpenStatistics(false);

        return nodeOptions;
    }

    public void notifyCache(HugeType type, Id id) {
        EventHub eventHub;
        if (type.isGraph()) {
            eventHub = this.params.graphEventHub();
        } else if (type.isSchema()) {
            eventHub = this.params.schemaEventHub();
        } else {
            return;
        }
        try {
            // How to avoid update cache from server info
            eventHub.notify(Events.CACHE, "invalid", type, id);
        } catch (RejectedExecutionException e) {
            LOG.warn("Can't update cache due to EventHub is too busy");
        }
    }

    public PeerId endpoint() {
        PeerId endpoint = new PeerId();
        String endpointStr = this.config().get(CoreOptions.RAFT_ENDPOINT);
        if (!endpoint.parse(endpointStr)) {
            throw new HugeException("Failed to parse endpoint %s", endpointStr);
        }
        return endpoint;
    }

    public boolean isSafeRead() {
        return this.config().get(CoreOptions.RAFT_SAFE_READ);
    }

    public ExecutorService snapshotExecutor() {
        return this.snapshotExecutor;
    }

    public ExecutorService backendExecutor() {
        return this.backendExecutor;
    }

    public GraphMode graphMode() {
        return this.params.mode();
    }

    private HugeConfig config() {
        return this.params.configuration();
    }

    private RpcServer initAndStartRpcServer() {
        Whitebox.setInternalState(
                 BoltRaftRpcFactory.class, "CHANNEL_WRITE_BUF_LOW_WATER_MARK",
                 this.config().get(CoreOptions.RAFT_RPC_BUF_LOW_WATER_MARK));
        Whitebox.setInternalState(
                 BoltRaftRpcFactory.class, "CHANNEL_WRITE_BUF_HIGH_WATER_MARK",
                 this.config().get(CoreOptions.RAFT_RPC_BUF_HIGH_WATER_MARK));

        PeerId serverId = new PeerId();
        serverId.parse(this.config().get(CoreOptions.RAFT_ENDPOINT));
        RpcServer rpcServer = RaftRpcServerFactory.createAndStartRaftRpcServer(
                                                   serverId.getEndpoint());
        LOG.info("RPC server is started successfully");
        return rpcServer;
    }

    private ExecutorService createReadIndexExecutor(int coreThreads) {
        int maxThreads = coreThreads << 2;
        String name = "store-read-index-callback";
        RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
        return newPool(coreThreads, maxThreads, name, handler);
    }

    private ExecutorService createSnapshotExecutor(int coreThreads) {
        int maxThreads = coreThreads << 2;
        String name = "store-snapshot-executor";
        RejectedExecutionHandler handler;
        handler = new ThreadPoolExecutor.CallerRunsPolicy();
        return newPool(coreThreads, maxThreads, name, handler);
    }

    private ExecutorService createBackendExecutor(int threads) {
        String name = "store-backend-executor";
        RejectedExecutionHandler handler;
        handler = new ThreadPoolExecutor.CallerRunsPolicy();
        return newPool(threads, threads, name, handler);
    }

    private static ExecutorService newPool(int coreThreads, int maxThreads,
                                           String name,
                                           RejectedExecutionHandler handler) {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        return ThreadPoolUtil.newBuilder()
                             .poolName(name)
                             .enableMetric(false)
                             .coreThreads(coreThreads)
                             .maximumThreads(maxThreads)
                             .keepAliveSeconds(300L)
                             .workQueue(workQueue)
                             .threadFactory(new NamedThreadFactory(name, true))
                             .rejectedHandler(handler)
                             .build();
    }
}
