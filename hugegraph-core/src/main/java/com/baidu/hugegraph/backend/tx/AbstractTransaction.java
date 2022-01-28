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

package com.baidu.hugegraph.backend.tx;

import java.nio.ByteBuffer;
import java.util.Set;

import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.exception.NotAllowException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.Transaction;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.kafka.ClusterRole;
import com.baidu.hugegraph.kafka.producer.ProducerClient;
import com.baidu.hugegraph.kafka.producer.StandardProducerBuilder;
import com.baidu.hugegraph.kafka.topic.HugeGraphSyncTopic;
import com.baidu.hugegraph.kafka.topic.HugeGraphSyncTopicBuilder;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.collection.IdSet;
import com.google.common.util.concurrent.RateLimiter;

public abstract class AbstractTransaction implements Transaction {

    protected static final Logger LOG = Log.logger(Transaction.class);

    private final Thread ownerThread = Thread.currentThread();

    private boolean autoCommit = false;
    private boolean closed = false;
    private boolean committing = false;
    private boolean committing2Backend = false;

    private final HugeGraphParams graph;
    private final BackendStore store;

    private BackendMutation mutation;

    protected final AbstractSerializer serializer;

    protected final ProducerClient<String, ByteBuffer> producer;

    public AbstractTransaction(HugeGraphParams graph, BackendStore store) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(store, "store");

        this.graph = graph;
        this.serializer = this.graph.serializer();

        this.producer = new StandardProducerBuilder().build();
    
        this.store = store;
        this.reset();

        store.open(this.graph.configuration());
    }

    public HugeGraph graph() {
        E.checkNotNull(this.graph, "graph");
        return this.graph.graph();
    }

    protected HugeGraphParams params() {
        E.checkNotNull(this.graph, "graph");
        return this.graph;
    }

    protected BackendStore store() {
        E.checkNotNull(this.store, "store");
        return this.store;
    }

    public BackendFeatures storeFeatures() {
        return this.store.features();
    }

    public boolean storeInitialized() {
        return this.store.initialized();
    }

    public <R> R metadata(HugeType type, String meta, Object... args) {
        return this.store().metadata(type, meta, args);
    }

    public String graphName() {
        return this.params().name();
    }

    public GraphMode graphMode() {
        return this.params().mode();
    }

    @Watched(prefix = "tx")
    public Number queryNumber(Query query) {
        LOG.debug("Transaction queryNumber: {}", query);

        E.checkArgument(query.aggregate() != null,
                        "The aggregate must be set for number query: %s",
                        query);
        Query squery = this.serializer.writeQuery(query);

        this.beforeRead();
        try {
            return this.store.queryNumber(squery);
        } finally {
            this.afterRead();
        }
    }

    @Watched(prefix = "tx")
    public QueryResults<BackendEntry> query(Query query) {
        LOG.debug("Transaction query: {}", query);
        /*
         * NOTE: it's dangerous if an IdQuery/ConditionQuery is empty
         * check if the query is empty and its class is not the Query itself
         */
        if (query.empty() && !query.getClass().equals(Query.class)) {
            throw new BackendException("Query without any id or condition");
        }

        Query squery = this.serializer.writeQuery(query);

        // Do rate limit if needed
        RateLimiter rateLimiter = this.graph.readRateLimiter();
        if (rateLimiter != null && query.resultType().isGraph()) {
            double time = rateLimiter.acquire(1);
            if (time > 0) {
                LOG.debug("Waited for {}s to query", time);
            }
        }
        BackendEntryIterator.checkInterrupted();

        this.beforeRead();
        try {
            this.injectOlapPkIfNeeded(squery);
            return new QueryResults<>(this.store.query(squery), query);
        } finally {
            this.afterRead(); // TODO: not complete the iteration currently
        }
    }

    private void injectOlapPkIfNeeded(Query query) {
        if (!query.resultType().isVertex() ||
            !this.graph.readMode().showOlap()) {
            return;
        }
        /*
         * Control olap access by auth, only accessible olap property key
         * will be queried
         */
        Set<Id> olapPks = new IdSet(CollectionType.EC);
        for (PropertyKey propertyKey : this.graph.graph().propertyKeys()) {
            if (propertyKey.olap()) {
                olapPks.add(propertyKey.id());
            }
        }
        query.olapPks(olapPks);
    }

    @Watched(prefix = "tx")
    public BackendEntry query(HugeType type, Id id) {
        IdQuery idQuery = new IdQuery(type, id);
        return this.query(idQuery).one();
    }

    public BackendEntry get(HugeType type, Id id) {
        BackendEntry entry = this.query(type, id);
        if (entry == null) {
            throw new NotFoundException(
                      "Not found the %s entry with id '%s'",
                      type.readableName(), id);
        }
        return entry;
    }

    @Watched(prefix = "tx")
    @Override
    public void commit() throws BackendException {
        LOG.debug("Transaction commit() [auto: {}]...", this.autoCommit);
        this.checkOwnerThread();

        if (this.closed) {
            throw new BackendException("Transaction has been closed");
        }

        if (this.committing) {
            // It is not allowed to recursively commit in a transaction
            return;
        }

        if (!this.hasUpdate()) {
            LOG.debug("Transaction has no data to commit({})", store());
            return;
        }

        // Do rate limit if needed
        RateLimiter rateLimiter = this.graph.writeRateLimiter();
        if (rateLimiter != null) {
            int size = this.mutationSize();
            double time = size > 0 ? rateLimiter.acquire(size) : 0.0;
            if (time > 0) {
                LOG.debug("Waited for {}s to mutate {} item(s)", time, size);
            }
        }
        BackendEntryIterator.checkInterrupted();

        // Do commit
        assert !this.committing : "Not allowed to commit when it's committing";
        this.committing = true;
        try {
            this.commit2Backend();
        } finally {
            this.committing = false;
            this.reset();
        }
    }

    @Override
    public void commitIfGtSize(int size) throws BackendException {
        if (this.mutationSize() >= size) {
            this.commit();
        }
    }

    @Watched(prefix = "tx")
    @Override
    public void rollback() throws BackendException {
        LOG.debug("Transaction rollback()...");
        this.reset();
        if (this.committing2Backend) {
            this.rollbackBackend();
        }
    }

    @Watched(prefix = "tx")
    @Override
    public void close() {
        if (this.hasUpdate()) {
            throw new BackendException("There are still changes to commit");
        }
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.autoCommit = true; /* Let call after close() fail to commit */
        this.store().close();
    }

    @Override
    public boolean autoCommit() {
        return this.autoCommit;
    }

    public boolean hasUpdate() {
        return !this.mutation.isEmpty();
    }

    public boolean hasUpdate(HugeType type, Action action) {
        return this.mutation.contains(type, action);
    }

    public int mutationSize() {
        return this.mutation.size();
    }

    protected void autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    protected void reset() {
        if (this.mutation == null || !this.mutation.isEmpty()) {
            this.mutation = new BackendMutation();
        }
    }

    protected BackendMutation mutation() {
        return this.mutation;
    }

    protected void commit2Backend() {
        BackendMutation mutation = this.prepareCommit();
        assert !mutation.isEmpty();
        this.commitMutation2Backend(mutation);
    }

    protected void commitMutation2Backend(BackendMutation... mutations) {
        assert mutations.length > 0;
        this.committing2Backend = true;

        String clusterRole = this.graph.clusterRole();
        Boolean needToSync = clusterRole.equals(ClusterRole.MASTER.toString());

        // If an exception occurred, catch in the upper layer and rollback
        this.store.beginTx();
        for (BackendMutation mutation : mutations) {
            this.store.mutate(mutation);

            if (needToSync) {
                HugeGraphSyncTopic topic = new HugeGraphSyncTopicBuilder()
                    .setGraphName(this.graphName())
                    .setGraphSpace(this.graph().graphSpace())
                    .setMutation(mutation)
                    .build();
                this.producer.produce(topic);
            }
        }

        

        this.store.commitTx();
  
        this.committing2Backend = false;
    }

    protected void rollbackBackend() {
        this.committing2Backend = false;
        this.store.rollbackTx();
        // TODO kafka rollback 
    }

    protected BackendMutation prepareCommit() {
        // For sub-class preparing data, nothing to do here
        LOG.debug("Transaction prepareCommit()...");
        return this.mutation();
    }

    protected void beforeWrite() {
        // TODO: auto open()
    }

    protected void afterWrite() {
        if (this.autoCommit()) {
            this.commitOrRollback();
        }
    }

    protected void beforeRead() {
        if (this.autoCommit() && this.hasUpdate()) {
            this.commitOrRollback();
        }
    }

    protected void afterRead() {
        // pass
    }

    protected void checkOwnerThread() {
        if (Thread.currentThread() != this.ownerThread) {
            throw new BackendException("Can't operate a tx in other threads");
        }
    }

    @Watched(prefix = "tx")
    public void commitOrRollback() {
        LOG.debug("Transaction commitOrRollback()");
        this.checkOwnerThread();

        /*
         * The mutation will be reset after commit, in order to log the
         * mutation after failure, let's save it to a local variable.
         */
        BackendMutation mutation = this.mutation();

        try {
            // Do commit
            this.commit();
        } catch (Throwable e1) {
            LOG.error("Failed to commit changes:", e1);
            // Do rollback
            try {
                this.rollback();
            } catch (Throwable e2) {
                LOG.error("Failed to rollback changes:\n {}", mutation, e2);
            }
            /*
             * Rethrow the commit exception
             * The e.getMessage maybe too long to see key information,
             */
            throw new BackendException("Failed to commit changes: %s(%s)",
                                       StringUtils.abbreviateMiddle(
                                       e1.getMessage(), ".", 256),
                                       HugeException.rootCause(e1));
        }
    }

    @Watched(prefix = "tx")
    public void doInsert(BackendEntry entry) {
        this.doAction(Action.INSERT, entry);
    }

    @Watched(prefix = "tx")
    public void doAppend(BackendEntry entry) {
        this.doAction(Action.APPEND, entry);
    }

    @Watched(prefix = "tx")
    public void doEliminate(BackendEntry entry) {
        this.doAction(Action.ELIMINATE, entry);
    }

    @Watched(prefix = "tx")
    public void doRemove(BackendEntry entry) {
        this.doAction(Action.DELETE, entry);
    }

    protected void doAction(Action action, BackendEntry entry) {
        LOG.debug("Transaction {} entry {}", action, entry);
        E.checkNotNull(entry, "entry");
        this.mutation.add(entry, action);
    }

    protected void allowedOlapQuery(ConditionQuery query) {
        if (!this.graph().readMode().showOlap()) {
            for (Id pkId : query.userpropKeys()) {
                PropertyKey propertyKey = this.graph().propertyKey(pkId);
                if (propertyKey.olap()) {
                    throw new NotAllowException(
                            "Not allowed to query by olap property key '%s'" +
                            " when graph-read-mode is '%s'",
                            propertyKey, this.graph().readMode());
                }
            }
        }
    }

    @Watched(prefix = "tx")
    public void applyMutation(BackendMutation mutation) {
        this.mutation = mutation;
    }
}
