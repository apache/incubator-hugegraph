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

import java.util.Iterator;

import org.slf4j.Logger;
import com.baidu.hugegraph.util.Log;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.Transaction;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.MutateAction;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

public abstract class AbstractTransaction implements Transaction {

    protected static final Logger LOG = Log.logger(Transaction.class);

    private Thread ownerThread = Thread.currentThread();
    private boolean autoCommit = false;
    private boolean closed = false;

    private final HugeGraph graph;
    private final BackendStore store;

    private BackendMutation mutation;

    protected AbstractSerializer serializer;

    public AbstractTransaction(HugeGraph graph, BackendStore store) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(store, "store");

        this.graph = graph;
        this.serializer = this.graph.serializer();

        this.store = store;
        this.reset();

        store.open(graph.configuration());
    }

    public HugeGraph graph() {
        E.checkNotNull(this.graph, "graph");
        return this.graph;
    }

    public BackendStore store() {
        E.checkNotNull(this.graph, "store");
        return this.store;
    }

    public Object metadata(HugeType type, String meta, Object... args) {
        return this.store().metadata(type, meta, args);
    }

    public Iterable<BackendEntry> query(Query query) {
        LOG.debug("Transaction query: {}", query);
        /*
         * NOTE: it's dangerous if an IdQuery/ConditionQuery is empty
         * check if the query is empty and its class is not the Query itself
         */
        if (query.empty() && !query.getClass().equals(Query.class)) {
            throw new BackendException("Query without any id or condition");
        }

        query = this.serializer.writeQuery(query);

        this.beforeRead();
        Iterable<BackendEntry> result = this.store.query(query);
        this.afterRead();

        return result;
    }

    public BackendEntry query(HugeType type, Id id) {
        IdQuery q = new IdQuery(type, id);
        Iterator<BackendEntry> results = this.query(q).iterator();
        if (results.hasNext()) {
            BackendEntry entry = results.next();
            assert !results.hasNext();
            return entry;
        }
        return null;
    }

    public BackendEntry get(HugeType type, Id id) {
        BackendEntry entry = query(type, id);
        if (entry == null) {
            throw new NotFoundException(
                      "Not found the %s entry with id '%s'", type, id);
        }
        return entry;
    }

    @Override
    public void commit() throws BackendException {
        LOG.debug("Transaction commit() [auto: {}]...", this.autoCommit);
        this.checkOwnerThread();

        if (this.closed) {
            throw new BackendException("Transaction has been closed");
        }

        this.prepareCommit();

        BackendMutation mutation = this.mutation();
        if (mutation.isEmpty()) {
            LOG.debug("Transaction has no data to commit({})", this.store());
            return;
        }

        // If an exception occurred, catch in the upper layer and roll back
        this.store.beginTx();
        this.store.mutate(mutation);
        this.reset();
        this.store.commitTx();
    }

    @Override
    public void rollback() throws BackendException {
        LOG.debug("Transaction rollback()...");
        this.reset();
        this.store.rollbackTx();
    }

    @Override
    public boolean autoCommit() {
        return this.autoCommit;
    }

    public void autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public void beforeWrite() {
        // TODO: auto open()
    }

    @Override
    public void afterWrite() {
        if (autoCommit()) {
            this.commitOrRollback();
        }
    }

    @Override
    public void beforeRead() {
        // TODO: auto open()
        if (this.hasUpdates()) {
            this.commitOrRollback();
        }
    }

    @Override
    public void afterRead() {
        // Pass
    }

    @Override
    public void close() {
        this.closed = true;
        this.autoCommit = true; /* Let call after close() fail to commit */
        this.store().close();
    }

    protected void reset() {
        this.mutation = new BackendMutation();
    }

    protected BackendMutation mutation() {
        return this.mutation;
    }

    protected void prepareCommit() {
        // For sub-class preparing data, nothing to do here
        LOG.debug("Transaction prepareCommit()...");
    }

    protected void commitOrRollback() {
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
            // Rethrow the commit exception
            throw new BackendException(
                      "Failed to commit changes: %s", e1.getMessage());
        }
    }

    protected void checkOwnerThread() {
        if (Thread.currentThread() != this.ownerThread) {
            throw new BackendException("Can't operate a tx in other threads");
        }
    }

    public void addEntry(BackendEntry entry) {
        LOG.debug("Transaction add entry {}", entry);
        E.checkNotNull(entry, "entry");
        E.checkNotNull(entry.id(), "entry id");

        this.mutation.add(entry, MutateAction.INSERT);
    }

    public void removeEntry(BackendEntry entry) {
        LOG.debug("Transaction remove entry {}", entry);
        E.checkNotNull(entry, "entry");
        E.checkNotNull(entry.id(), "entry id");

        this.mutation.add(entry, MutateAction.DELETE);
    }

    public void removeEntry(HugeType type, Id id) {
        this.removeEntry(this.serializer.writeId(type, id));
    }

    public void appendEntry(BackendEntry entry) {
        LOG.debug("Transaction append entry {}", entry);
        E.checkNotNull(entry, "entry");
        E.checkNotNull(entry.id(), "entry id");

        this.mutation.add(entry, MutateAction.APPEND);
    }

    public void eliminateEntry(BackendEntry entry) {
        LOG.debug("Transaction eliminate entry {}", entry);
        E.checkNotNull(entry, "entry");
        E.checkNotNull(entry.id(), "entry id");

        this.mutation.add(entry, MutateAction.ELIMINATE);
    }

    public boolean hasUpdates() {
        return !this.mutation.isEmpty();
    }
}
