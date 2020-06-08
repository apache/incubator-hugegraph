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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;

public final class RaftSharedContext {

    private final HugeConfig config;
    private final RpcServer rpcServer;
    private final ExecutorService readIndexExecutor;
    private final ExecutorService snapshotExecutor;

    public RaftSharedContext(HugeConfig config) {
        this.config = config;
        this.rpcServer = this.initAndStartRpcServer();
//        this.readIndexExecutor = this.createReadIndexExecutor(4);
        this.readIndexExecutor = null;
        this.snapshotExecutor = this.createSnapshotExecutor(4);
    }

    public HugeConfig config() {
        return this.config;
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

    public ExecutorService createSnapshotExecutor(int coreThreads) {
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
