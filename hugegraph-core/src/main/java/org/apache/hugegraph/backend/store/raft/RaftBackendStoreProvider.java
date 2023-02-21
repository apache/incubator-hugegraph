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

package org.apache.hugegraph.backend.store.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;
import com.google.common.collect.ImmutableSet;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.store.AbstractBackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.raft.rpc.ListPeersProcessor;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.backend.store.raft.rpc.RpcForwarder;
import org.apache.hugegraph.backend.store.raft.rpc.SetLeaderProcessor;
import org.apache.hugegraph.backend.store.raft.rpc.StoreCommandProcessor;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class RaftBackendStoreProvider implements BackendStoreProvider {

    private static final Logger LOG = Log.logger(RaftBackendStoreProvider.class);

    private final AbstractBackendStoreProvider provider;
    private final HugeGraphParams params;
    private final RpcForwarder rpcForwarder;
    private Map<Short, RaftContext> raftContexts;
    private RaftBackendStore schemaStore;
    private RaftBackendStore graphStore;
    private RaftBackendStore systemStore;

    public RaftBackendStoreProvider(HugeGraphParams params,
                                    AbstractBackendStoreProvider provider) {
        this.provider = provider;
        this.schemaStore = null;
        this.graphStore = null;
        this.systemStore = null;
        this.params = params;
        this.raftContexts = new HashMap<>();
        PeerId peerId = new PeerId();
        peerId.parse(this.params.configuration().get(CoreOptions.RAFT_ENDPOINT));
        this.rpcForwarder = new RpcForwarder(peerId);
    }

    public RaftGroupManager raftNodeManager() {
        // TODO
        //return this.context().raftNodeManager();
        return null;
    }

    private Set<RaftBackendStore> stores() {
        return ImmutableSet.of(this.schemaStore, this.graphStore,
                               this.systemStore);
    }

    private void checkOpened() {
        E.checkState(this.graph() != null &&
                     this.schemaStore != null &&
                     this.graphStore != null &&
                     this.systemStore != null,
                     "The RaftBackendStoreProvider has not been opened");
    }

    private void checkNonSharedStore(BackendStore store) {
        E.checkArgument(!store.features().supportsSharedStorage(),
                        "Can't enable raft mode with %s backend",
                        this.type());
    }

    @Override
    public String type() {
        return this.provider.type();
    }

    @Override
    public String driverVersion() {
        return this.provider.driverVersion();
    }

    @Override
    public String storedVersion() {
        return this.provider.storedVersion();
    }

    @Override
    public String graph() {
        return this.provider.graph();
    }

    public Map<Short, String> routeTable = null;

    @Override
    public synchronized BackendStore loadSchemaStore(HugeConfig config) {
        Integer shardNum = config.get(CoreOptions.RAFT_SHARD_NUM);
        if (this.schemaStore == null) {
            LOG.info("Init raft backend schema store");
            Map<Short, BackendStore> stores = new HashMap<>();
            for (int i = 0; i < shardNum; i++) {
                if (stores.containsKey(i)) {
                    continue;
                }
                BackendStore store = this.provider.newSchemaStore(config, SCHEMA_STORE);
                this.checkNonSharedStore(store);
                stores.putIfAbsent((short) i, store);
            }

            this.schemaStore = new RaftBackendStore(stores, this.raftContexts, shardNum,
                                                    config.get(CoreOptions.RAFT_ENDPOINT),
                                                    this.rpcForwarder);
            initRaftContextByRouteTable();
            this.schemaStore.setRouteTable(this.routeTable);
            this.raftContexts.forEach((shardId, raftContext) -> {
                raftContext.addStore(StoreType.SCHEMA, this.schemaStore);
            });
        }

        return this.schemaStore;
    }

    public void initRaftContextByRouteTable() {
        HugeConfig config = this.params.configuration();
        this.schemaStore.open(config);
        this.schemaStore.init();
        this.routeTable = this.schemaStore.readRoute();
        if (this.routeTable.size() == 0) {
            initRaftRouteTable();
        }
        for (Map.Entry<Short, String> shard : this.routeTable.entrySet()) {
            String peers = shard.getValue();
            // init RaftContext if endpoint in routeTable
            if (peers.contains(config.get(CoreOptions.RAFT_ENDPOINT))) {
                this.raftContexts.put(shard.getKey(), new RaftContext(this.params, peers));
            }
        }
    }

    public void initRaftRouteTable() {
        HugeConfig config = this.params.configuration();
        String raftGroupPeers = config.get(CoreOptions.RAFT_GROUP_PEERS);
        String[] peers = raftGroupPeers.split(",");
        Integer shardNum = config.get(CoreOptions.RAFT_SHARD_NUM);
        Integer replicationNum = config.get(CoreOptions.RAFT_REPLICATION_NUM);
        int peersLen = peers.length;
        Map<Short, String> routeTable = new HashMap<>(peersLen);
        int lastPosition = -1;
        for (int i = 0; i < shardNum; i++) {
            StringBuilder peersStr = new StringBuilder();
            for (int j = 0; j < replicationNum; j++) {
                lastPosition = (1 + lastPosition) % peersLen;
                peersStr.append(peers[lastPosition]).append(",");
            }
            peersStr.deleteCharAt(peersStr.length() - 1);
            routeTable.put((short) i, peersStr.toString());
        }
        this.schemaStore.writeRoute(routeTable);
        this.routeTable = routeTable;
    }

    @Override
    public synchronized BackendStore loadGraphStore(HugeConfig config) {
        Integer shardNum = config.get(CoreOptions.RAFT_SHARD_NUM);
        if (this.graphStore == null) {
            LOG.info("Init raft backend graph store");
            Map<Short, BackendStore> stores = new HashMap<>();
            for (int i = 0; i < shardNum; i++) {
                if (stores.containsKey(i)) {
                    continue;
                }
                BackendStore store = this.provider.newGraphStore(config, GRAPH_STORE);
                this.checkNonSharedStore(store);
                stores.put((short) i, store);
            }
            this.graphStore = new RaftBackendStore(stores, this.raftContexts, shardNum,
                                                   config.get(CoreOptions.RAFT_ENDPOINT),
                                                   this.rpcForwarder);
            this.graphStore.setRouteTable(this.routeTable);

            this.raftContexts.forEach((shardId, raftContext) -> {
                raftContext.addStore(StoreType.GRAPH, this.graphStore);
            });
        }
        return this.graphStore;
    }

    @Override
    public synchronized BackendStore loadSystemStore(HugeConfig config) {
        if (this.systemStore == null) {
            Integer shardNum = config.get(CoreOptions.RAFT_SHARD_NUM);
            LOG.info("Init raft backend system store");
            Map<Short, BackendStore> stores = new HashMap<>();
            for (int i = 0; i < shardNum; i++) {
                if (stores.containsKey(i)) {
                    continue;
                }
                BackendStore store = this.provider.newSystemStore(config, SYSTEM_STORE);
                this.checkNonSharedStore(store);
                stores.put((short) i, store);
            }
            this.systemStore = new RaftBackendStore(stores, this.raftContexts, shardNum,
                                                    config.get(CoreOptions.RAFT_ENDPOINT),
                                                    this.rpcForwarder);
            this.systemStore.setRouteTable(this.routeTable);

            this.raftContexts.forEach((shardId, raftContext) -> {
                raftContext.addStore(StoreType.SYSTEM, this.systemStore);
            });
        }
        return this.systemStore;
    }

    @Override
    public void open(String name) {
        this.provider.open(name);
    }

    @Override
    public void waitReady(RpcServer rpcServer) {
        com.alipay.sofa.jraft.rpc.RpcServer wrapRpcServer = registerRpcRequestProcessors(rpcServer);
        PeerId peerId = new PeerId();
        peerId.parse(this.params.configuration().get(CoreOptions.RAFT_ENDPOINT));

        for (Map.Entry<Short, RaftContext> raftContext : this.raftContexts.entrySet()) {
            RaftContext context = raftContext.getValue();
            context.initRaftNode(wrapRpcServer, raftContext.getKey(), peerId);
            context.waitRaftNodeStarted();
        }
        try {
            // TODO improve: check if other nodes has been Started.
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private com.alipay.sofa.jraft.rpc.RpcServer registerRpcRequestProcessors(RpcServer rpcServer) {
        com.alipay.sofa.jraft.rpc.RpcServer wrapRpcServer = wrapRpcServer(rpcServer);
        wrapRpcServer.registerProcessor(new StoreCommandProcessor(this.raftContexts));
        wrapRpcServer.registerProcessor(new SetLeaderProcessor(this.raftContexts));
        wrapRpcServer.registerProcessor(new ListPeersProcessor(this.raftContexts));
        return wrapRpcServer;
    }

    private com.alipay.sofa.jraft.rpc.RpcServer wrapRpcServer(
        com.alipay.remoting.rpc.RpcServer rpcServer) {
        HugeConfig config = this.params.configuration();
        // TODO: pass ServerOptions instead of CoreOptions, to share by graphs
        Integer lowWaterMark = config.get(
            CoreOptions.RAFT_RPC_BUF_LOW_WATER_MARK);
        System.setProperty("bolt.channel_write_buf_low_water_mark",
                           String.valueOf(lowWaterMark));
        Integer highWaterMark = config.get(
            CoreOptions.RAFT_RPC_BUF_HIGH_WATER_MARK);
        System.setProperty("bolt.channel_write_buf_high_water_mark",
                           String.valueOf(highWaterMark));

        // Reference from RaftRpcServerFactory.createAndStartRaftRpcServer
        com.alipay.sofa.jraft.rpc.RpcServer raftRpcServer = new BoltRpcServer(rpcServer);
        RaftRpcServerFactory.addRaftRequestProcessors(raftRpcServer);
        return raftRpcServer;
    }

    @Override
    public void close() {
        this.provider.close();
        this.raftContexts.forEach((shardId, raftContext) -> raftContext.close());
        // rpcClient need to be shutdown or initGraph won't be stopped.
        this.rpcForwarder.close();
    }

    @Override
    public void init() {
        this.checkOpened();
        for (RaftBackendStore store : this.stores()) {
            store.init();
        }
        this.notifyAndWaitEvent(Events.STORE_INIT);

        LOG.debug("Graph '{}' store has been initialized", this.graph());
    }

    @Override
    public void clear() {
        this.checkOpened();
        for (RaftBackendStore store : this.stores()) {
            // Just clear tables of store, not clear space
            store.clear(false);
        }
        for (RaftBackendStore store : this.stores()) {
            // Only clear space of store
            store.clear(true);
        }
        this.notifyAndWaitEvent(Events.STORE_CLEAR);

        LOG.debug("Graph '{}' store has been cleared", this.graph());
    }

    @Override
    public void truncate() {
        this.checkOpened();
        for (RaftBackendStore store : this.stores()) {
            store.truncate();
        }
        this.notifyAndWaitEvent(Events.STORE_TRUNCATE);
        LOG.debug("Graph '{}' store has been truncated", this.graph());
        /*
         * Take the initiative to generate a snapshot, it can avoid this
         * situation: when the server restart need to read the database
         * (such as checkBackendVersionInfo), it happens that raft replays
         * the truncate log, at the same time, the store has been cleared
         * (truncate) but init-store has not been completed, which will
         * cause reading errors.
         * When restarting, load the snapshot first and then read backend,
         * will not encounter such an intermediate state.
         */
        this.createSnapshot();
    }

    @Override
    public boolean initialized() {
        Optional<Map.Entry<Short, RaftContext>> context =
            this.raftContexts.entrySet().stream().findFirst();
        if (context.isPresent()) {
            // check RaftNode if has been init
            return context.get().getValue().node() != null && this.provider.initialized();
        }
        return false;
    }

    @Override
    public void createSnapshot() {
        // TODO: snapshot for StoreType.ALL instead of StoreType.GRAPH
        Map.Entry<Short, RaftContext> raftContext =
            raftContexts.entrySet().stream().findFirst().get();
        StoreCommand command = new StoreCommand(StoreType.GRAPH,
                                                StoreAction.SNAPSHOT, null, raftContext.getKey());
        RaftStoreClosure closure = new RaftStoreClosure(command);
        RaftClosure<?> future = raftContext.getValue().node().submitAndWait(command, closure);
        E.checkState(future != null, "The snapshot future can't be null");
        try {
            future.waitFinished();
            LOG.debug("Graph '{}' has writed snapshot", this.graph());
        } catch (Throwable e) {
            throw new BackendException("Failed to create snapshot", e);
        }
    }

    @Override
    public void onCloneConfig(HugeConfig config, String newGraph) {
        this.provider.onCloneConfig(config, newGraph);
    }

    @Override
    public void onDeleteConfig(HugeConfig config) {
        this.provider.onDeleteConfig(config);
    }

    @Override
    public void resumeSnapshot() {
        // Jraft doesn't expose API to load snapshot
        throw new UnsupportedOperationException("resumeSnapshot");
    }

    @Override
    public void listen(EventListener listener) {
        this.provider.listen(listener);
    }

    @Override
    public void unlisten(EventListener listener) {
        this.provider.unlisten(listener);
    }

    @Override
    public EventHub storeEventHub() {
        return this.provider.storeEventHub();
    }

    protected final void notifyAndWaitEvent(String event) {
        Future<?> future = this.storeEventHub().notify(event, this);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: {}", event, e);
        }
    }
}
