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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Log;

public class RaftBackendStore implements BackendStore {

    private static final Logger LOG = Log.logger(RaftBackendStore.class);

    private final BackendStore store;
    private final RaftSharedContext context;
    private final ThreadLocal<MutationBatch> threadLocalBatch;

    public RaftBackendStore(BackendStore store, RaftSharedContext context) {
        this.store = store;
        this.context = context;
        this.threadLocalBatch = new ThreadLocal<>();
    }

    private String group() {
        return this.database() + "-" + this.store();
    }

    private RaftNode node() {
        return this.context.node(this.group());
    }

    @Override
    public String store() {
        return this.store.store();
    }

    @Override
    public String database() {
        return this.store.database();
    }

    @Override
    public BackendStoreProvider provider() {
        return this.store.provider();
    }

    @Override
    public boolean isSchemaStore() {
        return this.store.isSchemaStore();
    }

    @Override
    public synchronized void open(HugeConfig config) {
        this.store.open(config);
        this.initRaftNodeIfNeeded();
    }

    public void waitStoreStarted() {
        RaftNode node = this.node();
        node.waitLeaderElected(RaftSharedContext.WAIT_LEADER_TIMEOUT);
        if (node.node().isLeader()) {
            node.waitStarted(RaftSharedContext.NO_TIMEOUT);
        }
    }

    private void initRaftNodeIfNeeded() {
        this.context.addNode(this.group(), this.store);
    }

    @Override
    public void close() {
        this.store.close();
    }

    @Override
    public boolean opened() {
        return this.store.opened();
    }

    @Override
    public void init() {
        // this.submitAndWait(StoreCommand.INIT);
        this.store.init();
    }

    @Override
    public void clear(boolean clearSpace) {
        byte[] bytes = new byte[]{clearSpace ? (byte) 1 : (byte) 0};
        this.submitAndWait(StoreAction.CLEAR, bytes);
    }

    @Override
    public boolean initialized() {
        return this.store.initialized();
    }

    @Override
    public void truncate() {
        this.submitAndWait(StoreAction.TRUNCATE);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        // Just add to local buffer
        this.getOrNewBatch().add(mutation);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<BackendEntry> query(Query query) {
        return (Iterator<BackendEntry>) this.queryByRaft(
                                        query, o -> store.query(query));
    }

    @Override
    public Number queryNumber(Query query) {
        return (Number) this.queryByRaft(query, o -> store.queryNumber(query));
    }

    @Override
    public void beginTx() {
    }

    @Override
    public void commitTx() {
        MutationBatch batch = this.getOrNewBatch();
        try {
            byte[] bytes = StoreSerializer.writeMutations(batch.mutations);
            this.submitAndWait(StoreAction.COMMIT_TX, bytes);
        } finally {
            batch.clear();
        }
    }

    @Override
    public void rollbackTx() {
        this.submitAndWait(StoreAction.ROLLBACK_TX);
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        return this.store.metadata(type, meta, args);
    }

    @Override
    public BackendFeatures features() {
        return this.store.features();
    }

    @Override
    public void increaseCounter(HugeType type, long increment) {
        IncrCounter incrCounter = new IncrCounter(type, increment);
        byte[] bytes = StoreSerializer.writeIncrCounter(incrCounter);
        this.submitAndWait(StoreAction.INCR_COUNTER, bytes);
    }

    @Override
    public long getCounter(HugeType type) {
        return this.store.getCounter(type);
    }

    @Override
    public void writeSnapshot(String snapshotPath) {
        this.store.writeSnapshot(snapshotPath);
    }

    @Override
    public void readSnapshot(String snapshotPath) {
        this.store.readSnapshot(snapshotPath);
    }

    private Object submitAndWait(StoreAction action) {
        return this.submitAndWait(new StoreCommand(action));
    }

    private Object submitAndWait(StoreAction action, byte[] data) {
        return this.submitAndWait(new StoreCommand(action, data));
    }

    private Object submitAndWait(StoreCommand command) {
        StoreClosure closure = new StoreClosure(command);
        return this.node().submitAndWait(command, closure);
    }

    private Object queryByRaft(Query query, Function<Object, Object> func) {
        if (!this.context.isSafeRead()) {
            return func.apply(query);
        }

        StoreCommand command = new StoreCommand(StoreAction.QUERY);
        StoreClosure closure = new StoreClosure(command);
        ReadIndexClosure readIndexClosure = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    closure.complete(status, () -> func.apply(query));
                } else {
                    closure.failure(status, new BackendException(
                            "Failed to execute query '%s' with 'ReadIndex': %s",
                            query, status));
                }
            }
        };
        this.node().node().readIndex(BytesUtil.EMPTY_BYTES, readIndexClosure);
        try {
            return closure.waitFinished();
        } catch (Throwable t) {
            LOG.warn("Failed to execute query {} with 'ReadIndex': {}",
                     query, closure.status());
            throw new BackendException("Failed to execute query", t);
        }
    }

    private static class MutationBatch {

        private List<BackendMutation> mutations;

        public MutationBatch() {
            this.mutations = new ArrayList<>();
        }

        public void add(BackendMutation mutation) {
            this.mutations.add(mutation);
        }

        public void clear() {
            this.mutations = new ArrayList<>();
        }
    }

    private MutationBatch getOrNewBatch() {
        MutationBatch batch = this.threadLocalBatch.get();
        if (batch == null) {
            batch = new MutationBatch();
            this.threadLocalBatch.set(batch);
        }
        return batch;
    }

    public static class IncrCounter {

        private HugeType type;
        private long increment;

        public IncrCounter(HugeType type, long increment) {
            this.type = type;
            this.increment = increment;
        }

        public HugeType type() {
            return this.type;
        }

        public long increment() {
            return this.increment;
        }
    }
}
