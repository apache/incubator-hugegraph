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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.raft.RaftBackendStore.IncrCounter;
import com.baidu.hugegraph.util.Log;

public class StoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = Log.logger(StoreStateMachine.class);

    private NodeId nodeId;
    private final BackendStore store;
    private final RaftSharedContext context;
    private final StoreSnapshotFile snapshotFile;
    private final AtomicLong leaderTerm;
    private final Map<Byte, Function<BytesBuffer, Object>> funcs;

    public StoreStateMachine(BackendStore store, RaftSharedContext context) {
        this.nodeId = nodeId;
        this.store = store;
        this.context = context;
        this.snapshotFile = new StoreSnapshotFile();
        this.leaderTerm = new AtomicLong(-1);
        this.funcs = new ConcurrentHashMap<>();
        this.registerCommands();
    }

    private void registerCommands() {
        // StoreCommand.register(StoreCommand.INIT, this.store::init);
        this.register(StoreCommand.TRUNCATE, this.store::truncate);
        this.register(StoreCommand.BEGIN_TX, this.store::beginTx);
        this.register(StoreCommand.COMMIT_TX, this.store::commitTx);
        this.register(StoreCommand.ROLLBACK_TX, this.store::rollbackTx);
        // clear
        this.register(StoreCommand.CLEAR, buffer -> {
            boolean clearSpace = buffer.read() > 0;
            this.store.clear(clearSpace);
            return null;
        });
        // mutate
        this.register(StoreCommand.MUTATE, buffer -> {
            BackendMutation m = StoreSerializer.deserializeMutation(buffer);
            this.store.mutate(m);
            return null;
        });
        // increase counter
        this.register(StoreCommand.INCR_COUNTER, buffer -> {
            IncrCounter counter = StoreSerializer.deserializeIncrCounter(buffer);
            this.store.increaseCounter(counter.type(), counter.increment());
            return null;
        });
    }

    private void register(byte cmd, Function<BytesBuffer, Object> func) {
        this.funcs.put(cmd, func);
    }

    private void register(byte cmd, Runnable runnable) {
        this.funcs.put(cmd, s -> {
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
        while (iter.hasNext()) {
            byte command;
            BytesBuffer buffer;
            StoreClosure closure = (StoreClosure) iter.done();
            if (closure != null) {
                // Leader just take it out from the closure
                command = closure.command().command();
                buffer = BytesBuffer.wrap(closure.command().data());
            } else {
                // Follower need deserializeMutation data
                buffer = BytesBuffer.wrap(iter.getData());
                command = buffer.read();
            }

            try {
                Object data = this.applyCommand(command, buffer);
                success(closure, data);
            } catch (Exception e) {
                failure(closure, e);
            }
            iter.next();
        }
    }

    private Object applyCommand(byte command, BytesBuffer buffer)
                                throws Exception {
        Function<BytesBuffer, Object> func = this.funcs.get(command);
        Object result = func.apply(buffer);
        LOG.info("The store {} performed command {}", this.store, command);
        return result;
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
        LOG.debug("The node {} become to leader", this.nodeId);
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(Status status) {
        LOG.debug("The node {} abdicated from leader", this.nodeId);
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

    private boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    private static void success(StoreClosure closure, Object data) {
        if (closure != null) {
            // closure is null on follower node
            closure.complete(data);
        }
    }

    private static void failure(StoreClosure closure, Exception e) {
        if (closure != null) {
            closure.failure(e);
        }
    }
}
