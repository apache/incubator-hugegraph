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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;

public final class RaftSharedComponent {

    private final HugeConfig config;
    private final RpcServer rpcServer;
    private final Executor readIndexExecutor;

    public RaftSharedComponent(HugeConfig config) {
        this.config = config;
        this.rpcServer = this.initAndStartRpcServer();
        this.readIndexExecutor = this.initReadIndexExecutor();
    }

    public HugeConfig config() {
        return this.config;
    }

    public RpcServer rpcServer() {
        return this.rpcServer;
    }

    public Executor readIndexExecutor() {
        return this.readIndexExecutor;
    }

    private RpcServer initAndStartRpcServer() {
        PeerId serverId = new PeerId();
        serverId.parse(this.config.get(CoreOptions.RAFT_PEERID));
        return RaftRpcServerFactory.createAndStartRaftRpcServer(
                                    serverId.getEndpoint());
    }

    private Executor initReadIndexExecutor() {
        return createReadIndexExecutor(Math.max(Utils.cpus() << 2, 4));
    }

    private static ExecutorService createReadIndexExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        final RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
        return newPool(coreThreads, maxThreads, "kvstore-read-index-callback", handler);
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads,
                                           final String name,
                                           final RejectedExecutionHandler handler) {
        final BlockingQueue<Runnable> defaultWorkQueue = new SynchronousQueue<>();
        return newPool(coreThreads, maxThreads, defaultWorkQueue, name, handler);
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads,
                                           final BlockingQueue<Runnable> workQueue,
                                           final String name,
                                           final RejectedExecutionHandler handler) {
        return ThreadPoolUtil.newBuilder() //
                             .poolName(name) //
                             .enableMetric(true) //
                             .coreThreads(coreThreads) //
                             .maximumThreads(maxThreads) //
                             .keepAliveSeconds(60L) //
                             .workQueue(workQueue) //
                             .threadFactory(new NamedThreadFactory(name, true))
                             .rejectedHandler(handler) //
                             .build();
    }
}
