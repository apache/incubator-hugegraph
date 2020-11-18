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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.raft.RaftBackendStore.IncrCounter;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.util.LZ4Util;
import com.baidu.hugegraph.util.Log;

public class StoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = Log.logger(StoreStateMachine.class);

    private final RaftSharedContext context;
    private final StoreSnapshotFile snapshotFile;
    private final Map<StoreAction, BiConsumer<BackendStore, BytesBuffer>> funcs;

    public StoreStateMachine(RaftSharedContext context) {
        this.context = context;
        this.snapshotFile = new StoreSnapshotFile(context.stores());
        this.funcs = new EnumMap<>(StoreAction.class);
        this.registerCommands();
    }

    private BackendStore store(StoreType type) {
        return this.context.originStore(type);
    }

    private RaftNode node() {
        return this.context.node();
    }

    private void registerCommands() {
        this.register(StoreAction.TRUNCATE, (store, buffer) -> {
            store.truncate();
        });
        // clear
        this.register(StoreAction.CLEAR, (store, buffer) -> {
            boolean clearSpace = buffer.read() > 0;
            store.clear(clearSpace);
        });
        this.register(StoreAction.BEGIN_TX, (store, buffer) -> store.beginTx());
        this.register(StoreAction.COMMIT_TX, (store, buffer) -> {
            List<BackendMutation> ms = StoreSerializer.readMutations(buffer);
            store.beginTx();
            for (BackendMutation mutation : ms) {
                store.mutate(mutation);
                // update cache on follower when graph run in general mode
                if (this.context.graphMode() == GraphMode.NONE) {
                    this.updateCacheIfNeeded(mutation);
                }
            }
            store.commitTx();
        });
        this.register(StoreAction.ROLLBACK_TX, (store, buffer) -> {
            store.rollbackTx();
        });
        // increase counter
        this.register(StoreAction.INCR_COUNTER, (store, buffer) -> {
            IncrCounter counter = StoreSerializer.readIncrCounter(buffer);
            store.increaseCounter(counter.type(), counter.increment());
        });
    }

    private void updateCacheIfNeeded(BackendMutation mutation) {
        // Only follower need to update cache from store to tx
        if (this.node().selfIsLeader()) {
            return;
        }
        for (HugeType type : mutation.types()) {
            if (!type.isGraph() && !type.isSchema()) {
                continue;
            }
            for (java.util.Iterator<BackendAction> it = mutation.mutation(type);
                 it.hasNext();) {
                BackendEntry entry = it.next().entry();
                this.context.notifyCache(type, entry.originId());
            }
        }
    }

    private void register(StoreAction action,
                          BiConsumer<BackendStore, BytesBuffer> func) {
        this.funcs.put(action, func);
    }

    @Override
    public void onApply(Iterator iter) {
        LOG.debug("Node role: {}", this.node().selfIsLeader() ?
                                   "leader" : "follower");
        StoreClosure closure = null;
        try {
            while (iter.hasNext()) {
                closure = (StoreClosure) iter.done();
                if (closure != null) {
                    // Leader just take it out from the closure
                    BytesBuffer buffer = BytesBuffer.wrap(closure.command().data());
                    // The first two bytes are StoreType and StoreAction
                    StoreType type = StoreType.valueOf(buffer.read());
                    StoreAction action = StoreAction.valueOf(buffer.read());
                    // Let the producer thread to handle it
                    closure.complete(Status.OK(), () -> {
                        return this.applyCommand(type, action, buffer);
                    });
                } else {
                    // Follower need readMutation data
                    byte[] bytes = iter.getData().array();
                    // Follower seems no way to wait future
                    // Let the backend thread do it directly
                    this.context.backendExecutor().submit(() -> {
                        BytesBuffer buffer = LZ4Util.decompress(bytes,
                                             RaftSharedContext.BLOCK_SIZE);
                        buffer.forReadWritten();
                        StoreType type = StoreType.valueOf(buffer.read());
                        StoreAction action = StoreAction.valueOf(buffer.read());
                        try {
                            this.applyCommand(type, action, buffer);
                        } catch (Throwable e) {
                            LOG.error("Failed to execute backend command: {}",
                                      action, e);
                            throw new BackendException("Backend error", e);
                        }
                    });
                }
                iter.next();
            }
        } catch (Throwable e) {
            LOG.error("StateMachine occured critical error", e);
            Status status = new Status(RaftError.ESTATEMACHINE,
                                       "StateMachine occured critical error: %s",
                                       e.getMessage());
            if (closure != null) {
                closure.failure(status, e);
            }
            // Will cause current node inactive
            iter.setErrorAndRollback(1L, status);
        }
    }

    private Object applyCommand(StoreType type, StoreAction action,
                                BytesBuffer buffer) {
        BackendStore store = this.store(type);
        BiConsumer<BackendStore, BytesBuffer> func = this.funcs.get(action);
        func.accept(store, buffer);
        return null;
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        LOG.debug("The node {} start snapshot save", this.node().nodeId());
        this.snapshotFile.save(writer, done, this.context.snapshotExecutor());
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (this.node() != null && this.node().selfIsLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        return this.snapshotFile.load(reader);
    }

    @Override
    public void onLeaderStart(long term) {
        LOG.info("The node {} become to leader", this.node().nodeId());
        this.node().onLeaderInfoChange(this.node().nodeId(), true);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(Status status) {
        LOG.info("The node {} abdicated from leader", this.node().nodeId());
        this.node().onLeaderInfoChange(null, false);
        super.onLeaderStop(status);
    }

    @Override
    public void onStartFollowing(LeaderChangeContext ctx) {
        LOG.info("The node {} become to follower", this.node().nodeId());
        this.node().onLeaderInfoChange(ctx.getLeaderId(), false);
        super.onStartFollowing(ctx);
    }

    @Override
    public void onStopFollowing(LeaderChangeContext ctx) {
        LOG.info("The node {} abdicated from follower", this.node().nodeId());
        this.node().onLeaderInfoChange(null, false);
        super.onStopFollowing(ctx);
    }

    @Override
    public void onConfigurationCommitted(Configuration conf) {
        super.onConfigurationCommitted(conf);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e.getMessage(), e);
    }
}
