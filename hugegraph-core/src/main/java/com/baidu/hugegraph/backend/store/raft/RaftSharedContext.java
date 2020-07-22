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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public final class RaftSharedContext {

    private static final Logger LOG = Log.logger(RaftSharedContext.class);

    private final HugeConfig config;
    // It will be synchronized wrap
    private final Map<String, RaftNode> nodes;
    private final RpcServer rpcServer;
    private final ExecutorService readIndexExecutor;
    private final ExecutorService snapshotExecutor;

    public RaftSharedContext(HugeConfig config) {
        this.config = config;
        this.nodes = new HashMap<>();
        this.rpcServer = this.initAndStartRpcServer();
//        this.readIndexExecutor = this.createReadIndexExecutor(4);
        this.readIndexExecutor = null;
        if (this.config.get(CoreOptions.RAFT_USE_SNAPSHOT)) {
            this.snapshotExecutor = this.createSnapshotExecutor(4);
        } else {
            this.snapshotExecutor = null;
        }
    }

    public HugeConfig config() {
        return this.config;
    }

    public RaftNode node(String group) {
        return this.nodes.get(group);
    }

    public void addNode(String group, BackendStore store) {
        if (!this.nodes.containsKey(group)) {
            synchronized (this.nodes) {
                if (!this.nodes.containsKey(group)) {
                    LOG.info("Initing raft node for '{}'", group);
                    RaftNode node = new RaftNode(store, this);
                    this.nodes.put(group, node);
                }
            }
        }
    }

    public RpcServer rpcServer() {
        return this.rpcServer;
    }

    public ExecutorService readIndexExecutor() {
        return this.readIndexExecutor;
    }

    public ExecutorService snapshotExecutor() {
        return this.snapshotExecutor;
    }

    private RpcServer initAndStartRpcServer() {
        PeerId serverId = new PeerId();
        serverId.parse(this.config.get(CoreOptions.RAFT_PEERID));
        return RaftRpcServerFactory.createAndStartRaftRpcServer(
                                    serverId.getEndpoint());
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

    private static ExecutorService newPool(int coreThreads, int maxThreads,
                                           String name,
                                           RejectedExecutionHandler handler) {
        BlockingQueue<Runnable> workQueue = new SynchronousQueue<>();
        return ThreadPoolUtil.newBuilder()
                             .poolName(name)
                             .enableMetric(true)
                             .coreThreads(coreThreads)
                             .maximumThreads(maxThreads)
                             .keepAliveSeconds(60L)
                             .workQueue(workQueue)
                             .threadFactory(new NamedThreadFactory(name, true))
                             .rejectedHandler(handler)
                             .build();
    }
}
