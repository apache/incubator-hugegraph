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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.raft.RaftBackendStore.IncrCounter;
import com.baidu.hugegraph.util.CodeUtil;
import com.baidu.hugegraph.util.Log;

public class StoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = Log.logger(StoreStateMachine.class);

    private NodeId nodeId;
    private final BackendStore store;
    private final RaftSharedContext context;
    private final StoreSnapshotFile snapshotFile;
    private final AtomicLong leaderTerm;
    private final Map<StoreAction, Function<BytesBuffer, Object>> funcs;

    public StoreStateMachine(BackendStore store, RaftSharedContext context) {
        this.store = store;
        this.context = context;
        this.snapshotFile = new StoreSnapshotFile();
        this.leaderTerm = new AtomicLong(-1);
        this.funcs = new EnumMap<>(StoreAction.class);
        this.registerCommands();
    }

    private void registerCommands() {
        // StoreCommand.register(StoreCommand.INIT, this.store::init);
        this.register(StoreAction.TRUNCATE, this.store::truncate);
        // clear
        this.register(StoreAction.CLEAR, buffer -> {
            boolean clearSpace = buffer.read() > 0;
            this.store.clear(clearSpace);
            return null;
        });
        this.register(StoreAction.BEGIN_TX, this.store::beginTx);
        this.register(StoreAction.COMMIT_TX, buffer -> {
            List<BackendMutation> ms = StoreSerializer.readMutations(buffer);
            this.store.beginTx();
            for (BackendMutation mutation : ms) {
                this.store.mutate(mutation);
            }
            this.store.commitTx();
            return null;
        });
        this.register(StoreAction.ROLLBACK_TX, this.store::rollbackTx);
        // increase counter
        this.register(StoreAction.INCR_COUNTER, buffer -> {
            IncrCounter counter = StoreSerializer.readIncrCounter(buffer);
            this.store.increaseCounter(counter.type(), counter.increment());
            return null;
        });
    }

    private void register(StoreAction action,
                          Function<BytesBuffer, Object> func) {
        this.funcs.put(action, func);
    }

    private void register(StoreAction action, Runnable runnable) {
        this.funcs.put(action, s -> {
            runnable.run();
            return null;
        });
    }

    public void nodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void onApply(Iterator iter) {
        LOG.debug("Node role: {}", this.isLeader() ? "leader" : "follower");
        StoreClosure closure = null;
        try {
            while (iter.hasNext()) {
                StoreAction action;
                BytesBuffer buffer;
                closure = (StoreClosure) iter.done();
                if (closure != null) {
                    // Leader just take it out from the closure
                    action = closure.command().action();
                    buffer = BytesBuffer.wrap(closure.command().data());
                } else {
                    // Follower need readMutation data
                    buffer = CodeUtil.decompress(iter.getData().array());
                    action = StoreAction.fromCode(buffer.read());
                }
                if (closure != null) {
                    // Closure is null on follower node
                    // Let the producer thread to handle it
                    closure.complete(Status.OK(),
                                     () -> this.applyCommand(action, buffer));
                } else {
                    // Follower seems no way to wait future
                    // Let the backend thread do it directly
                    this.context.backendExecutor().submit(() -> {
                        try {
                            this.applyCommand(action, buffer);
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

    private Object applyCommand(StoreAction action, BytesBuffer buffer) {
        Function<BytesBuffer, Object> func = this.funcs.get(action);
        return func.apply(buffer);
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        LOG.debug("The node {} start snapshot save", this.nodeId);
        this.snapshotFile.save(this.store, writer, done,
                               this.context.snapshotExecutor());
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (this.isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        LOG.debug("The node {} start snapshot load", this.nodeId);
        return this.snapshotFile.load(this.store, reader);
    }

    @Override
    public void onLeaderStart(long term) {
        LOG.info("The node {} become to leader", this.nodeId);
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(Status status) {
        LOG.info("The node {} abdicated from leader", this.nodeId);
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e.getMessage(), e);
    }

    private boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }
}
