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

package org.apache.hugegraph.backend.store.raft;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.google.protobuf.ZeroByteStringHelper;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BinaryEntryIterator;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.SystemSchemaStore;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.backend.store.raft.rpc.RpcForwarder;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;

public class RaftBackendStore implements BackendStore {

    private static final Logger LOG = Log.logger(RaftBackendStore.class);

    private final int shardNum;
    private final String endpoint;
    private final RpcForwarder rpcForwarder;
    private final Map<Short, BackendStore> store;
    private final Map<Short, RaftContext> contexts;
    private final ThreadLocal<MutationBatch> mutationBatch;
    private Map<Short, String> raftRoute;

    public RaftBackendStore(Map<Short, BackendStore> store, Map<Short, RaftContext> contexts,
                            int shardNum, String endpoint, RpcForwarder rpcForwarder) {
        this.store = store;
        this.contexts = contexts;
        this.mutationBatch = new ThreadLocal<>();
        this.shardNum = shardNum;
        this.endpoint = endpoint;
        this.rpcForwarder = rpcForwarder;
    }

    public BackendStore originStore(Short shardId) {
        return this.store.get(shardId);
    }

    public BackendStore getStore() {
        return this.store.entrySet().stream().findFirst().get().getValue();
    }


    private RaftNode node(RaftContext raftContext) {
        RaftNode node = raftContext.node();
        E.checkState(node != null, "The raft node should be initialized first");
        return node;
    }

    @Override
    public void writeRoute(Map<Short, String> routeTable) {
        this.store.forEach((id, store) -> store.writeRoute(routeTable));
    }

    @Override
    public Map<Short, String> readRoute() {
        return this.getStore().readRoute();
    }

    public void setRouteTable(Map<Short, String> routeTable) {
        this.raftRoute = routeTable;
    }

    @Override
    public String store() {
        return this.getStore().store();
    }

    @Override
    public String storedVersion() {
        return this.getStore().storedVersion();
    }

    @Override
    public String database() {
        return this.getStore().database();
    }

    @Override
    public BackendStoreProvider provider() {
        return this.getStore().provider();
    }

    @Override
    public SystemSchemaStore systemSchemaStore() {
        return this.getStore().systemSchemaStore();
    }

    @Override
    public boolean isSchemaStore() {
        return this.getStore().isSchemaStore();
    }

    @Override
    public synchronized void open(HugeConfig config) {
        this.store.forEach((id, store) -> store.open(shardConfig(config, id)));
    }

    public HugeConfig shardConfig(HugeConfig config, int i) {
        if (i == 0) {
            return config;
        }
        String dataPath = String.format("%s_%s", config.getString("rocksdb.data_path"), i);
        HugeConfig newConfig = new HugeConfig(new PropertiesConfiguration());
        newConfig.copy(config);
        newConfig.setProperty("rocksdb.data_path", dataPath);
        newConfig.setProperty("rocksdb.wal_path", dataPath);
        newConfig.setProperty("rocksdb.sst_path", dataPath);
        return newConfig;
    }

    @Override
    public void close() {
        this.store.forEach((id, store) -> store.close());
    }

    @Override
    public boolean opened() {
        for (Map.Entry<Short, BackendStore> store : this.store.entrySet()) {
            if (!store.getValue().opened()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void init() {
        // this.submitAndWait(StoreCommand.INIT);
        this.store.forEach((id, store) -> store.init());
    }

    @Override
    public void clear(boolean clearSpace) {
        byte value = clearSpace ? (byte) 1 : (byte) 0;
        byte[] bytes = StoreCommand.wrap(value);
        HashMap<Short, byte[]> idAndBytes = new HashMap<>();
        for (Map.Entry<Short, String> route : this.raftRoute.entrySet()) {
            idAndBytes.put(route.getKey(), bytes);
        }
        this.submitAndWait(StoreAction.CLEAR, idAndBytes);
    }

    @Override
    public boolean initialized() {
        for (Map.Entry<Short, BackendStore> store : this.store.entrySet()) {
            if (!store.getValue().initialized()){
                return false;
            }
        }
        return true;
    }

    @Override
    public void truncate() {
        HashMap<Short, byte[]> idAndBytes = new HashMap<>();
        for (Map.Entry<Short, String> route : this.raftRoute.entrySet()) {
            idAndBytes.put(route.getKey(), null);
        }
        this.submitAndWait(StoreAction.TRUNCATE, idAndBytes);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (mutation.isEmpty()) {
            return;
        }
        // Just add to local buffer
        this.getOrNewBatch().add(mutation, this.shardNum);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<BackendEntry> query(Query query) {
        // merge results
        List<Object> results =
            this.queryByRaft(query, (o, i) -> this.originStore(i).query(query));

        BinaryEntryIterator backendEntryIterator = null;
        for (int i = 0; i < results.size(); i++) {
            Iterator<BackendEntry> binaryEntryIterator = (Iterator<BackendEntry>) results.get(i);
            if (i == 0) {
                backendEntryIterator = (BinaryEntryIterator) binaryEntryIterator;
            } else {
                if (binaryEntryIterator instanceof BinaryEntryIterator &&
                    ((BinaryEntryIterator<?>) binaryEntryIterator).result().hasNext()) {
                    backendEntryIterator.addResults(
                        ((BinaryEntryIterator<?>) binaryEntryIterator).result());
                } else if (binaryEntryIterator.hasNext()) {
                    backendEntryIterator.addResults(binaryEntryIterator);
                }
            }
        }
        return backendEntryIterator;
    }


    @Override
    public Number queryNumber(Query query) {
        return (Number)
            this.queryByRaft(query, (o, i) -> this.originStore(i).queryNumber(query));
    }

    @Override
    public void beginTx() {
        // Don't write raft log, commitTx(in StateMachine) will call beginTx
    }

    @Override
    public void commitTx() {
        MutationBatch batch = this.getOrNewBatch();
        try {
            Map<Short, byte[]> bytes = StoreSerializer.writeMutations(batch.mutations);
            this.submitAndWait(StoreAction.COMMIT_TX, bytes);
        } finally {
            batch.clear();
        }
    }

    @Override
    public void rollbackTx() {
        HashMap<Short, byte[]> idAndBytes = new HashMap<>();
        for (Map.Entry<Short, String> route : this.raftRoute.entrySet()) {
            idAndBytes.put(route.getKey(), null);
        }
        this.submitAndWait(StoreAction.ROLLBACK_TX, idAndBytes);
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        return this.getStore().metadata(type, meta, args);
    }

    @Override
    public BackendFeatures features() {
        return this.getStore().features();
    }

    @Override
    public void increaseCounter(HugeType type, long increment) {
        IncrCounter incrCounter = new IncrCounter(type, increment);
        byte[] bytes = StoreSerializer.writeIncrCounter(incrCounter);
        Map<Short, byte[]> idAndBytes = new HashMap<>();
        raftRoute.forEach((aShort, s) -> idAndBytes.put(aShort, bytes));
        this.submitAndWait(StoreAction.INCR_COUNTER, idAndBytes);
    }

    @Override
    public long getCounter(HugeType type) {
        Object counter = this.queryByRaft(type,
                                          this.contexts.entrySet().stream().findFirst().get()
                                                       .getValue(),
                                          (o, i) -> this.originStore(i).getCounter(type));

        assert counter instanceof Long;
        return (Long) counter;
    }

    private Object submitAndWait(StoreAction action, Map<Short, byte[]> dataMap) {
        for (Map.Entry<Short, byte[]> data : dataMap.entrySet()) {
            Short shardId = data.getKey();
            StoreCommand command = new StoreCommand(this.getStoreType(), action, data.getValue(),
                                                    shardId);

            RaftStoreClosure closure = new RaftStoreClosure(command);

            if (!this.contexts.containsKey(shardId)) {
                PeerId peer = getRandomPeer(this.raftRoute.get(shardId));
                LOG.debug("forward submit, shardId {}, peer {}, data {} ", shardId,
                          peer.getEndpoint().toString(), data.getValue());
                this.rpcForwarder.forwardToLeader(peer, command, closure);
            } else {
                LOG.debug("local submit, shardId {},  data {} ", shardId,
                          data.getValue());
                this.submitAndWait(this.contexts.get(shardId), command);
            }
        }
        return null;
    }

    private StoreType getStoreType() {
        if (BackendStoreProvider.SCHEMA_STORE.equals(this.store())) {
            return StoreType.SCHEMA;
        } else if (BackendStoreProvider.GRAPH_STORE.equals(this.store())) {
            return StoreType.GRAPH;
        } else {
            assert BackendStoreProvider.SYSTEM_STORE.equals(this.store());
            return StoreType.SYSTEM;
        }
    }

    private Object submitAndWait(RaftContext raftContext, StoreCommand command) {
        RaftStoreClosure closure = new RaftStoreClosure(command);
        return this.node(raftContext).submitAndWait(command, closure);
    }

    private static short getShardId(Id id, Integer shardNum) {
        int hashId = Arrays.hashCode(id.asBytes()) & Integer.MAX_VALUE; //make value not minus
        return (short) (hashId % shardNum);
    }

    private List<Object> queryByRaft(Query query, BiFunction<Object, Short, Object> func) {
        Collection<Id> ids = query.ids();
        Map<Short, String> forwardQuery = new HashMap<>();
        Map<Short, String> localQuery = new HashMap<>();

        if (ids.isEmpty()) {
            for (Map.Entry<Short, String> entry : this.raftRoute.entrySet()) {
                if (!entry.getValue().contains(this.endpoint)) {
                    forwardQuery.put(entry.getKey(), entry.getValue());
                } else {
                    localQuery.put(entry.getKey(), entry.getValue());
                }
            }
        }

        for (Id id : ids) {
            short shardId = getShardId(id, this.shardNum);
            LOG.info("QueryByRaft shardId {}", shardId);
            if (!this.contexts.containsKey(shardId)) {
                forwardQuery.put(shardId, this.raftRoute.get(shardId));
            } else {
                localQuery.put(shardId, this.raftRoute.get(shardId));
            }
        }

        List<Object> resultSet = new ArrayList<>();
        for (Map.Entry<Short, String> entry : localQuery.entrySet()) {
            LOG.info("【本地查询】: shardId {}, {}", entry.getKey(), query);
            RaftContext raftContext = contexts.get(entry.getKey());
            Object result = this.queryByRaft(query, raftContext, func);
            resultSet.add(result);
        }

        // TODO how to build an empty BinaryEntryIterator
        if (localQuery.entrySet().size() == 0) {
            RaftContext raftContext = contexts.entrySet().stream().findFirst().get().getValue();
            Object result = this.queryByRaft(query, raftContext, func);
            resultSet.add(result);
        }

        for (Map.Entry<Short, String> entry : forwardQuery.entrySet()) {
            PeerId peer = getRandomPeer(entry.getValue());
            RaftClosure<RaftRequests.StoreCommandResponse> raftClosure =
                this.rpcForwarder.forwardToQuery(peer, query, entry.getKey(),
                                                 this.getStoreType());
            try {
                RaftRequests.StoreCommandResponse response = raftClosure.waitFinished();
                if (response != null) {
                    List<BackendEntry> r = (List<BackendEntry>) new ObjectInputStream(
                        new ByteArrayInputStream(
                            ZeroByteStringHelper.getByteArray(response.getData())))
                        .readObject();
                    resultSet.add(r.iterator());
                }
            } catch (Throwable e) {
                throw new BackendException("Failed to wait store command %s", e);
            }
        }
        return resultSet;
    }

    private PeerId getRandomPeer(String peers) {
        // TODO check peers
        String[] peersArr = peers.split(",");
        String[] peer = peersArr[new Random().nextInt(peersArr.length)].split(":");
        return new PeerId(peer[0], Integer.parseInt(peer[1]));
    }

    // Long Number Iterator<BackendEntry>
    private Object queryByRaft(Object query, RaftContext raftContext,
                               BiFunction<Object, Short, Object> func) {
        if (!raftContext.safeRead()) {
            return func.apply(query, raftContext.shardId());
        }

        RaftClosure<Object> future = new RaftClosure<>();
        ReadIndexClosure readIndexClosure = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    future.complete(status, () -> func.apply(query, raftContext.shardId()));
                } else {
                    future.failure(status, new BackendException(
                                           "Failed to do raft read-index: %s",
                                           status));
                }
            }
        };
        this.node(raftContext).readIndex(BytesUtil.EMPTY_BYTES, readIndexClosure);
        try {
            return future.waitFinished();
        } catch (Throwable e) {
            LOG.warn("Failed to execute query '{}': {}",
                     query, future.status(), e);
            throw new BackendException("Failed to execute query: %s", e, query);
        }
    }

    private MutationBatch getOrNewBatch() {
        MutationBatch batch = this.mutationBatch.get();
        if (batch == null) {
            batch = new MutationBatch();
            this.mutationBatch.set(batch);
        }
        return batch;
    }

    private static class MutationBatch {

        // This object will stay in memory for a long time
        private final Map<Short, List<BackendMutation>> mutations;

        public MutationBatch() {
            this.mutations = new HashMap<>();
        }

        public void add(BackendMutation mutation, Integer shardNum) {
            Iterator<BackendAction> backendActionIterator = mutation.mutation();
            while (backendActionIterator.hasNext()) {
                BinaryBackendEntry entry =
                    (BinaryBackendEntry) backendActionIterator.next().entry();
                short shardId = getShardId(entry.id(), shardNum);
                List<BackendMutation> backendMutations = this.mutations
                    .getOrDefault(shardId, new ArrayList<>((int) Query.COMMIT_BATCH));
                backendMutations.add(mutation);
                this.mutations.put(shardId, backendMutations);
            }
        }

        public void clear() {
            this.mutations.clear();
        }
    }

    protected static final class IncrCounter {

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
