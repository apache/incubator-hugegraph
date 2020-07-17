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

import org.slf4j.Logger;

import com.alipay.sofa.jraft.rpc.RpcServer;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.util.Log;

public class RaftBackendStoreProvider implements BackendStoreProvider {

    private static final Logger LOG = Log.logger(RaftBackendStoreProvider.class);

    private final BackendStoreProvider provider;
    private final RaftSharedContext context;
    private RaftBackendStore schemaStore;
    private RaftBackendStore graphStore;
    private RaftBackendStore systemStore;

    public RaftBackendStoreProvider(BackendStoreProvider provider,
                                    HugeConfig config) {
        this.provider = provider;
        this.context = new RaftSharedContext(config);
        this.schemaStore = null;
        this.graphStore = null;
        this.systemStore = null;
        this.registerRpcRequestProcessors();
    }

    private void registerRpcRequestProcessors() {
        RpcServer rpcServer = this.context.rpcServer();
        rpcServer.registerProcessor(new StoreCommandRequestProcessor(
                                    this.context));
    }

    @Override
    public String type() {
        return this.provider.type();
    }

    @Override
    public String version() {
        return this.provider.version();
    }

    @Override
    public String graph() {
        return this.provider.graph();
    }

    @Override
    public synchronized BackendStore loadSchemaStore(final String name) {
        if (this.schemaStore == null) {
            LOG.info("Init raft backend schema store");
            BackendStore store = this.provider.loadSchemaStore(name);
            this.schemaStore = new RaftBackendStore(store, this.context);
        }
        return this.schemaStore;
    }

    @Override
    public synchronized BackendStore loadGraphStore(String name) {
        if (this.graphStore == null) {
            LOG.info("Init raft backend graph store");
            BackendStore store = this.provider.loadGraphStore(name);
            this.graphStore = new RaftBackendStore(store, this.context);
        }
        return this.graphStore;
    }

    @Override
    public synchronized BackendStore loadSystemStore(String name) {
        if (this.systemStore == null) {
            LOG.info("Init raft backend system store");
            BackendStore store = this.provider.loadSystemStore(name);
            this.systemStore = new RaftBackendStore(store, this.context);
        }
        return this.systemStore;
    }

    @Override
    public void open(String name) {
        this.provider.open(name);
    }

    @Override
    public void close() {
        this.provider.close();
        this.context.close();
    }

    @Override
    public void init() {
        this.provider.init();
    }

    @Override
    public void clear() {
        this.provider.clear();
    }

    @Override
    public void truncate() {
        this.provider.truncate();
    }

    @Override
    public void initSystemInfo(HugeGraph graph) {
        this.provider.initSystemInfo(graph);
    }

    @Override
    public void listen(EventListener listener) {
        this.provider.listen(listener);
    }

    @Override
    public void unlisten(EventListener listener) {
        this.provider.unlisten(listener);
    }
}
