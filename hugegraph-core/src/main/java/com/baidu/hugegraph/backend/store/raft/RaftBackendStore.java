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

import java.util.Iterator;

import org.slf4j.Logger;

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

    public RaftBackendStore(BackendStore store, RaftSharedContext context) {
        this.store = store;
        this.context = context;
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
        this.submitAndWait(StoreCommand.CLEAR, bytes);
    }

    @Override
    public boolean initialized() {
        return this.store.initialized();
    }

    @Override
    public void truncate() {
        this.submitAndWait(StoreCommand.TRUNCATE);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        // TODO: serialize can be lazy or no need for leader
        byte[] bytes = StoreSerializer.serializeMutation(mutation);
        this.submitAndWait(StoreCommand.MUTATE, bytes);
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        return this.store.query(query);
    }

//    TODO: support readOnlySafe, need serialize Query, it's a bit complicate
//    @Override
//    public Iterator<BackendEntry> query(Query query) {
//        boolean readOnlySafe = this.config().get(CoreOptions.RAFT_READ_SAFE);
//        if (!readOnlySafe) {
//            return this.store.query(query);
//        }
//
//        StoreCommand command = new StoreCommand(StoreCommand.QUERY);
//        StoreClosure queryClosure = new StoreClosure(command);
//        RaftNode raftNode = this.node();
//        raftNode.node().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
//            @Override
//            public void run(Status status, long index, byte[] reqCtx) {
//                if (status.isOk()) {
//                    queryClosure.complete(store.query(query));
//                    return;
//                }
//                component.readIndexExecutor().execute(() -> {
//                    if (raftNode.isLeader()) {
//                        LOG.warn("Failed to [query] with 'ReadIndex': {}, " +
//                                 "try to applying to the state machine.",
//                                 status);
//                        /*
//                         * If 'read index' read fails, try to applying to the
//                         * state machine at the leader node
//                         */
//                        byte[] bytes = QuerySerializer.serializeMutation(query);
//                        queryClosure.complete(submitAndWait(StoreCommand.QUERY,
//                                                            bytes));
//                    } else {
//                        LOG.warn("Failed to [query] with 'ReadIndex': {}.",
//                                 status);
//                        queryClosure.run(status);
//                    }
//                });
//            }
//        });
//        if (queryClosure.throwable() != null) {
//            throw new BackendException("Failed to query", queryClosure.future());
//        }
//        return (Iterator<BackendEntry>) queryClosure.data();
//    }

    @Override
    public Number queryNumber(Query query) {
        return this.store.queryNumber(query);
    }

    @Override
    public void beginTx() {
        this.submitAndWait(StoreCommand.BEGIN_TX);
    }

    @Override
    public void commitTx() {
        this.submitAndWait(StoreCommand.COMMIT_TX);
    }

    @Override
    public void rollbackTx() {
        this.submitAndWait(StoreCommand.ROLLBACK_TX);
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
        byte[] bytes = StoreSerializer.serializeIncrCounter(incrCounter);
        this.submitAndWait(StoreCommand.INCR_COUNTER, bytes);
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

    private Object submitAndWait(byte command) {
        return this.submitAndWait(new StoreCommand(command));
    }

    private Object submitAndWait(byte command, byte[] data) {
        return this.submitAndWait(new StoreCommand(command, data));
    }

    private Object submitAndWait(StoreCommand command) {
        StoreClosure closure = new StoreClosure(command);
        this.node().submitCommand(command, closure);
        // Here will wait future complete
        if (closure.throwable() != null) {
            throw new BackendException(closure.throwable());
        } else {
            return closure.data();
        }
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
